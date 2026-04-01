from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from github_miner import CommitCandidate
from task_generation import GeneratedTask

log = logging.getLogger("swe-eval.workspace")

_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_RESERVED_SOLUTION_NAMES = {"original"}


def _run(cmd: list[str], cwd: Path, timeout: int = 300) -> subprocess.CompletedProcess[str]:
    log.debug("Running command: %s (cwd=%s, timeout=%ss)", " ".join(cmd), cwd, timeout)
    return subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


@dataclass(slots=True)
class TaskPaths:
    name: str
    root: Path
    task_dir: Path
    original_dir: Path
    reference_dir: Path
    solutions_dir: Path
    comparisons_dir: Path
    evals_dir: Path
    commit_path: Path
    reference_patch_path: Path
    task_json_path: Path
    task_txt_path: Path


@dataclass(slots=True)
class SolutionPaths:
    task_name: str
    name: str
    root: Path
    repo_dir: Path
    solution_diff_path: Path
    solve_json_path: Path
    rollout_jsonl_path: Path


@dataclass(slots=True)
class EvalPaths:
    task_name: str
    name: str
    root: Path
    comparisons_dir: Path
    eval_json_path: Path


@dataclass(slots=True)
class ComparePaths:
    task_name: str
    name: str
    root: Path
    compare_json_path: Path


def validate_name(name: str, *, label: str) -> str:
    clean = name.strip()
    if not clean:
        raise ValueError(f"{label} name cannot be empty")
    if not _NAME_RE.fullmatch(clean):
        raise ValueError(
            f"{label} name {clean!r} is invalid; use only letters, numbers, '.', '-', and '_'",
        )
    return clean


def validate_solution_name(name: str, *, allow_reserved: bool = False) -> str:
    clean = validate_name(name, label="solution")
    if not allow_reserved and clean in _RESERVED_SOLUTION_NAMES:
        raise ValueError(f"solution name {clean!r} is reserved")
    return clean


def build_task_paths(tasks_root: Path, task_name: str) -> TaskPaths:
    task_name = validate_name(task_name, label="task")
    root = tasks_root / task_name
    task_dir = root / "task"
    return TaskPaths(
        name=task_name,
        root=root,
        task_dir=task_dir,
        original_dir=task_dir / "original",
        reference_dir=task_dir / "reference",
        solutions_dir=root / "solutions",
        comparisons_dir=root / "comparisons",
        evals_dir=root / "evals",
        commit_path=task_dir / "commit.json",
        reference_patch_path=task_dir / "reference.patch",
        task_json_path=task_dir / "task.json",
        task_txt_path=task_dir / "task.txt",
    )


def build_solution_paths(task_paths: TaskPaths, solution_name: str) -> SolutionPaths:
    solution_name = validate_solution_name(solution_name)
    root = task_paths.solutions_dir / solution_name
    return SolutionPaths(
        task_name=task_paths.name,
        name=solution_name,
        root=root,
        repo_dir=root / "repo",
        solution_diff_path=root / "solution.diff",
        solve_json_path=root / "solve.json",
        rollout_jsonl_path=root / "rollout.jsonl",
    )


def build_eval_paths(task_paths: TaskPaths, eval_name: str) -> EvalPaths:
    eval_name = validate_name(eval_name, label="eval")
    root = task_paths.evals_dir / eval_name
    return EvalPaths(
        task_name=task_paths.name,
        name=eval_name,
        root=root,
        comparisons_dir=root / "comparisons",
        eval_json_path=root / "eval.json",
    )


def build_compare_paths(task_paths: TaskPaths, compare_name: str) -> ComparePaths:
    compare_name = validate_name(compare_name, label="comparison")
    root = task_paths.comparisons_dir / compare_name
    return ComparePaths(
        task_name=task_paths.name,
        name=compare_name,
        root=root,
        compare_json_path=root / "compare.json",
    )


def materialize_task_workspace(tasks_root: Path, task_name: str, candidate: CommitCandidate) -> TaskPaths:
    task_paths = build_task_paths(tasks_root, task_name)
    log.debug(
        "Preparing task workspace for %s@%s under %s",
        candidate.repo_full_name,
        candidate.short_sha,
        task_paths.root,
    )
    if task_paths.root.exists():
        raise FileExistsError(f"Task {task_name!r} already exists at {task_paths.root}")

    task_paths.task_dir.mkdir(parents=True, exist_ok=False)
    task_paths.solutions_dir.mkdir(parents=True, exist_ok=False)
    task_paths.comparisons_dir.mkdir(parents=True, exist_ok=False)
    task_paths.evals_dir.mkdir(parents=True, exist_ok=False)

    clone_result = _run(
        ["git", "clone", "--filter=blob:none", "--no-checkout", candidate.repo_clone_url, str(task_paths.original_dir)],
        cwd=task_paths.task_dir,
        timeout=300,
    )
    if clone_result.returncode != 0:
        raise RuntimeError(f"git clone failed: {clone_result.stderr[-500:]}")

    fetch_result = _run(
        ["git", "fetch", "--depth=2", "origin", candidate.parent_sha, candidate.commit_sha],
        cwd=task_paths.original_dir,
        timeout=180,
    )
    if fetch_result.returncode != 0:
        raise RuntimeError(f"git fetch failed: {fetch_result.stderr[-500:]}")

    checkout_parent = _run(
        ["git", "checkout", "--force", candidate.parent_sha],
        cwd=task_paths.original_dir,
        timeout=120,
    )
    if checkout_parent.returncode != 0:
        raise RuntimeError(f"git checkout parent failed: {checkout_parent.stderr[-500:]}")
    ensure_tree_has_no_symlinks(
        task_paths.original_dir,
        label="original task tree",
    )

    shutil.copytree(task_paths.original_dir, task_paths.reference_dir, symlinks=True)

    checkout_reference = _run(
        ["git", "checkout", "--force", candidate.commit_sha],
        cwd=task_paths.reference_dir,
        timeout=120,
    )
    if checkout_reference.returncode != 0:
        raise RuntimeError(f"git checkout reference failed: {checkout_reference.stderr[-500:]}")
    ensure_tree_has_no_symlinks(
        task_paths.reference_dir,
        label="reference task tree",
    )

    log.debug("Writing task artifacts to %s", task_paths.task_dir)
    task_paths.commit_path.write_text(json.dumps(candidate.to_dict(), indent=2, sort_keys=True) + "\n")
    task_paths.reference_patch_path.write_text(candidate.combined_patch + "\n")
    return task_paths


def prepare_solution_workspace(task_paths: TaskPaths, solution_name: str) -> SolutionPaths:
    solution_paths = build_solution_paths(task_paths, solution_name)
    if solution_paths.root.exists():
        raise FileExistsError(
            f"Solution {solution_name!r} already exists for task {task_paths.name!r} at {solution_paths.root}",
        )
    solution_paths.root.mkdir(parents=True, exist_ok=False)
    shutil.copytree(task_paths.original_dir, solution_paths.repo_dir, symlinks=True)
    return solution_paths


def derive_eval_name(solution_names: list[str]) -> str:
    normalized = [validate_solution_name(name, allow_reserved=True) for name in solution_names]
    if len(normalized) < 2:
        raise ValueError("eval requires at least two solutions")
    return validate_name("--".join(normalized), label="eval")


def derive_compare_name(solution_names: list[str]) -> str:
    normalized = [validate_solution_name(name) for name in solution_names]
    if len(normalized) != 2:
        raise ValueError("compare requires exactly two solutions")
    return validate_name(f"{normalized[0]}--vs--{normalized[1]}", label="comparison")


def prepare_eval_workspace(task_paths: TaskPaths, eval_name: str) -> EvalPaths:
    eval_paths = build_eval_paths(task_paths, eval_name)
    if eval_paths.root.exists():
        raise FileExistsError(
            f"Eval {eval_name!r} already exists for task {task_paths.name!r} at {eval_paths.root}",
        )
    eval_paths.root.mkdir(parents=True, exist_ok=False)
    eval_paths.comparisons_dir.mkdir(parents=True, exist_ok=False)
    return eval_paths


def prepare_compare_workspace(task_paths: TaskPaths, compare_name: str) -> ComparePaths:
    compare_paths = build_compare_paths(task_paths, compare_name)
    if compare_paths.root.exists():
        raise FileExistsError(
            f"Comparison {compare_name!r} already exists for task {task_paths.name!r} at {compare_paths.root}",
        )
    compare_paths.root.mkdir(parents=True, exist_ok=False)
    return compare_paths


def resolve_task_paths(tasks_root: Path, task_name: str) -> TaskPaths:
    task_paths = build_task_paths(tasks_root, task_name)
    if not task_paths.root.exists():
        raise FileNotFoundError(f"Task {task_name!r} does not exist under {tasks_root}")
    if not task_paths.task_json_path.exists():
        raise FileNotFoundError(f"Task metadata is missing at {task_paths.task_json_path}")
    if not task_paths.commit_path.exists():
        raise FileNotFoundError(f"Commit metadata is missing at {task_paths.commit_path}")
    return task_paths


def resolve_solution_paths(task_paths: TaskPaths, solution_name: str) -> SolutionPaths:
    solution_paths = build_solution_paths(task_paths, solution_name)
    if not solution_paths.root.exists():
        raise FileNotFoundError(
            f"Solution {solution_name!r} does not exist for task {task_paths.name!r}",
        )
    if not solution_paths.solve_json_path.exists():
        raise FileNotFoundError(f"Solve result is missing at {solution_paths.solve_json_path}")
    if not solution_paths.solution_diff_path.exists():
        raise FileNotFoundError(f"Solution diff is missing at {solution_paths.solution_diff_path}")
    return solution_paths


def delete_task_workspace(tasks_root: Path, task_name: str) -> TaskPaths:
    task_paths = build_task_paths(tasks_root, task_name)
    if not task_paths.root.exists():
        raise FileNotFoundError(f"Task {task_name!r} does not exist under {tasks_root}")
    if not task_paths.root.is_dir():
        raise NotADirectoryError(f"Task path {task_paths.root} is not a directory")

    shutil.rmtree(task_paths.root)
    _prune_empty_workspace_dirs(tasks_root)
    return task_paths


def delete_all_task_workspaces(tasks_root: Path) -> list[str]:
    if not tasks_root.exists():
        return []
    if not tasks_root.is_dir():
        raise NotADirectoryError(f"Tasks root {tasks_root} is not a directory")

    deleted_names = sorted(entry.name for entry in tasks_root.iterdir() if entry.is_dir())
    shutil.rmtree(tasks_root)
    _prune_empty_workspace_dirs(tasks_root)
    return deleted_names


def load_commit_candidate(task_paths: TaskPaths) -> CommitCandidate:
    payload = read_json(task_paths.commit_path)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Commit metadata at {task_paths.commit_path} is invalid")
    return CommitCandidate.from_dict(payload)


def load_generated_task(task_paths: TaskPaths) -> GeneratedTask:
    payload = read_json(task_paths.task_json_path)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Task metadata at {task_paths.task_json_path} is invalid")
    task_payload = payload.get("task") if isinstance(payload.get("task"), dict) else payload
    return GeneratedTask.from_dict(task_payload)


def read_json(path: Path) -> object:
    return json.loads(path.read_text())


def ensure_tree_has_no_symlinks(root: Path, *, label: str) -> None:
    symlinks = find_tree_symlinks(root)
    if not symlinks:
        return

    sample = ", ".join(str(path) for path in symlinks[:5])
    if len(symlinks) > 5:
        sample = f"{sample}, ..."
    raise RuntimeError(f"{label} contains symbolic links, which are not allowed: {sample}")


def find_tree_symlinks(root: Path) -> list[Path]:
    symlinks: list[Path] = []
    for current_root, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        current_dir = Path(current_root)
        for name in [*dirnames, *filenames]:
            candidate = current_dir / name
            if candidate.is_symlink():
                symlinks.append(candidate.relative_to(root))
    return sorted(symlinks)


def git_diff(repo_dir: Path, *args: str) -> str:
    log.debug("Collecting git diff for %s", repo_dir)
    result = _run(["git", "diff", "--binary", *args], cwd=repo_dir, timeout=120)
    if result.returncode not in (0, 1):
        raise RuntimeError(f"git diff failed: {result.stderr[-500:]}")

    diff_output = result.stdout
    untracked_result = _run(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"],
        cwd=repo_dir,
        timeout=30,
    )
    if untracked_result.returncode != 0:
        raise RuntimeError(f"git ls-files failed: {untracked_result.stderr[-500:]}")

    for raw_path in [item for item in untracked_result.stdout.split("\0") if item]:
        file_result = _run(
            ["git", "diff", "--binary", "--no-index", "--", "/dev/null", raw_path],
            cwd=repo_dir,
            timeout=30,
        )
        if file_result.returncode not in (0, 1):
            raise RuntimeError(f"git diff for untracked file failed: {file_result.stderr[-500:]}")
        diff_output += file_result.stdout

    return diff_output


def git_changed_files(repo_dir: Path) -> list[str]:
    tracked_result = _run(["git", "diff", "--name-only", "--relative"], cwd=repo_dir, timeout=60)
    if tracked_result.returncode not in (0, 1):
        raise RuntimeError(f"git diff --name-only failed: {tracked_result.stderr[-500:]}")

    untracked_result = _run(
        ["git", "ls-files", "--others", "--exclude-standard"],
        cwd=repo_dir,
        timeout=30,
    )
    if untracked_result.returncode != 0:
        raise RuntimeError(f"git ls-files failed: {untracked_result.stderr[-500:]}")

    changed_paths = {
        line.strip()
        for line in tracked_result.stdout.splitlines() + untracked_result.stdout.splitlines()
        if line.strip()
    }
    return sorted(changed_paths)


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _prune_empty_workspace_dirs(tasks_root: Path) -> None:
    for path in (tasks_root, tasks_root.parent):
        if path.exists() and path.is_dir() and not any(path.iterdir()):
            path.rmdir()
