from __future__ import annotations

import argparse
import os
from pathlib import Path
from urllib.parse import urlparse

from config import DockerSolverAgentSource, RunConfig
from pipeline import delete_task_run, evaluate_task_run, generate_task_run, solve_task_run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate, solve, and evaluate SWE tasks as independent stages.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="Mine a commit and materialize a named task.")
    _add_shared_args(generate)
    generate.add_argument("--task", required=True, help="Unique name for the generated task.")
    generate.add_argument("--generator-model", help="Optional model override for task generation.")
    generate.add_argument(
        "--seed",
        type=int,
        help="Optional random seed for repeatable mining order.",
    )
    generate.add_argument(
        "--max-mining-attempts",
        type=int,
        default=25,
        help="How many GitHub event/commit retries to attempt while mining a task.",
    )

    solve = subparsers.add_parser("solve", help="Run a named task with a named solver agent.")
    _add_shared_args(solve)
    solve.add_argument("--task", required=True, help="Existing generated task name.")
    solve.add_argument("--solution", required=True, help="Unique name for this solver run.")
    solve.add_argument(
        "--agent",
        required=True,
        help="Local agent workspace directory or repo root, or a GitHub repo URL/shorthand whose agent lives in /agent.",
    )
    solve.add_argument("--solver-model", help="Optional model override for solving.")
    solve.add_argument(
        "--seed",
        type=int,
        help="Optional random seed for deterministic solver-side choices.",
    )
    solve.add_argument(
        "--solver-max-requests",
        type=int,
        help="Maximum number of proxied OpenRouter requests allowed for a solve run.",
    )
    solve.add_argument(
        "--solver-max-total-tokens",
        type=int,
        help="Maximum total OpenRouter tokens allowed for a solve run.",
    )
    solve.add_argument(
        "--solver-max-prompt-tokens",
        type=int,
        help="Maximum prompt tokens allowed for a solve run.",
    )
    solve.add_argument(
        "--solver-max-completion-tokens",
        type=int,
        help="Maximum completion tokens allowed for a solve run.",
    )
    solve.add_argument(
        "--solver-max-cost",
        type=float,
        help="Maximum OpenRouter cost allowed for a solve run.",
    )
    solve.add_argument(
        "--solver-max-tokens-per-request",
        type=int,
        help="Maximum completion tokens to allow on any single proxied request.",
    )
    solve.add_argument(
        "--docker-solver-image",
        help="Optional Docker image tag for the solver image. If omitted, one is derived.",
    )
    solve.add_argument(
        "--docker-solver-memory",
        default="2g",
        help="Docker memory limit for the solver container.",
    )
    solve.add_argument(
        "--docker-solver-cpus",
        default="2",
        help="Docker CPU limit for the solver container.",
    )
    solve.add_argument(
        "--docker-solver-pids-limit",
        type=int,
        default=256,
        help="Maximum number of processes allowed inside the solver container.",
    )
    solve.add_argument(
        "--docker-solver-tmp-size",
        default="128m",
        help="Maximum writable size of /tmp inside the solver container.",
    )
    solve.add_argument(
        "--docker-solver-workdir-size",
        default="2g",
        help="Maximum writable size of /work inside the solver container.",
    )
    solve.add_argument(
        "--docker-solver-nofile-limit",
        type=int,
        default=4096,
        help="Maximum number of open files allowed inside the solver container.",
    )
    solve.add_argument(
        "--docker-solver-max-output-bytes",
        type=int,
        default=1000000,
        help="Maximum combined stdout or stderr bytes allowed from the solver command before it is killed.",
    )
    solve.add_argument(
        "--docker-solver-user",
        help="Optional user to run the solver container as.",
    )
    solve.add_argument(
        "--docker-solver-keep-caps",
        action="store_true",
        help="Do not drop Linux capabilities in the solver container.",
    )
    solve.add_argument(
        "--docker-solver-allow-privilege-escalation",
        action="store_true",
        help="Do not set no-new-privileges on the solver container.",
    )
    solve.add_argument(
        "--docker-solver-writeable-rootfs",
        action="store_true",
        help="Do not force the solver container root filesystem to read-only mode.",
    )
    solve.add_argument(
        "--docker-solver-no-cache",
        action="store_true",
        help="Build the solver Docker image with --no-cache.",
    )

    evaluate = subparsers.add_parser("eval", help="Evaluate ordered solution pairs for one named task.")
    _add_shared_args(evaluate)
    evaluate.add_argument("--task", required=True, help="Existing generated task name.")
    evaluate.add_argument(
        "--solutions",
        required=True,
        nargs="+",
        help="Ordered solution names to compare. Supports '--solutions A B' and '--solutions A,B'.",
    )
    evaluate.add_argument("--eval-model", help="Optional model override for evaluation.")
    evaluate.add_argument(
        "--seed",
        type=int,
        help="Optional random seed for deterministic blind-candidate ordering.",
    )

    delete = subparsers.add_parser("delete", help="Delete saved task workspaces and related artifacts.")
    _add_shared_args(delete)
    delete.add_argument(
        "resource",
        nargs="?",
        choices=["task"],
        help="Optional resource type. Use 'task' for forms like 'tau delete task --all'.",
    )
    delete_group = delete.add_mutually_exclusive_group(required=True)
    delete_group.add_argument("--task", help="Delete one saved task by name.")
    delete_group.add_argument("--all", action="store_true", help="Delete all saved task workspaces.")
    return parser


def main() -> None:
    _load_dotenv()
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "generate":
            result = generate_task_run(task_name=args.task, config=_build_generate_config(args))
            print(f"generated {result.task_name}: {result.repo}@{result.commit_sha[:12]}")
            print(result.task_root)
            return
        if args.command == "solve":
            result = solve_task_run(
                task_name=args.task,
                solution_name=args.solution,
                config=_build_solve_config(args),
            )
            status = "success" if result.success else "failed"
            print(
                f"solved {result.task_name}/{result.solution_name}: "
                f"{result.repo}@{result.commit_sha[:12]} -> {status}"
            )
            print(result.solution_root)
            return
        if args.command == "eval":
            result = evaluate_task_run(
                task_name=args.task,
                solution_names=_normalize_solution_names(args.solutions),
                config=_build_eval_config(args),
            )
            print(
                f"evaluated {result.task_name}/{result.eval_name}: "
                f"{result.repo}@{result.commit_sha[:12]} -> {result.comparison_count} comparisons"
            )
            print(result.eval_root)
            return
        if args.command == "delete":
            result = delete_task_run(
                task_name=getattr(args, "task", None),
                delete_all=getattr(args, "all", False),
                config=_build_delete_config(args),
            )
            if result.deleted_all:
                print(f"deleted {result.deleted_count} task workspace(s)")
            else:
                print(f"deleted task {result.deleted_tasks[0]}")
            return
        parser.error(f"Unknown command: {args.command}")
    except Exception as exc:  # noqa: BLE001
        if getattr(args, "debug", False):
            raise
        parser.exit(1, f"error: {exc}\n")


def _add_shared_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--workspace-root",
        type=Path,
        default=Path.cwd(),
        help="Root directory that will receive workspace/tasks/... artifacts.",
    )
    parser.add_argument(
        "--agent-timeout",
        type=int,
        default=600,
        help="Timeout in seconds for each model or solver invocation.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug logging for the selected stage.",
    )


def _build_generate_config(args: argparse.Namespace) -> RunConfig:
    return RunConfig(
        workspace_root=args.workspace_root.resolve(),
        generator_model=args.generator_model,
        agent_timeout=args.agent_timeout,
        random_seed=args.seed,
        max_mining_attempts=args.max_mining_attempts,
        debug=args.debug,
    )


def _build_solve_config(args: argparse.Namespace) -> RunConfig:
    agent = _resolve_agent_source(args.agent, cwd=Path.cwd())
    return RunConfig(
        workspace_root=args.workspace_root.resolve(),
        solver_model=args.solver_model,
        agent_timeout=args.agent_timeout,
        solver_max_requests=args.solver_max_requests,
        solver_max_total_tokens=args.solver_max_total_tokens,
        solver_max_prompt_tokens=args.solver_max_prompt_tokens,
        solver_max_completion_tokens=args.solver_max_completion_tokens,
        solver_max_cost=args.solver_max_cost,
        solver_max_tokens_per_request=args.solver_max_tokens_per_request,
        random_seed=args.seed,
        solver_backend="docker-pi",
        docker_solver_image=args.docker_solver_image,
        docker_solver_agent=agent,
        docker_solver_memory=args.docker_solver_memory,
        docker_solver_cpus=args.docker_solver_cpus,
        docker_solver_pids_limit=args.docker_solver_pids_limit,
        docker_solver_tmp_size=args.docker_solver_tmp_size,
        docker_solver_workdir_size=args.docker_solver_workdir_size,
        docker_solver_nofile_limit=args.docker_solver_nofile_limit,
        docker_solver_max_output_bytes=args.docker_solver_max_output_bytes,
        docker_solver_drop_caps=not args.docker_solver_keep_caps,
        docker_solver_no_new_privileges=not args.docker_solver_allow_privilege_escalation,
        docker_solver_read_only_rootfs=not args.docker_solver_writeable_rootfs,
        docker_solver_user=args.docker_solver_user,
        docker_solver_no_cache=args.docker_solver_no_cache,
        debug=args.debug,
    )


def _build_eval_config(args: argparse.Namespace) -> RunConfig:
    return RunConfig(
        workspace_root=args.workspace_root.resolve(),
        eval_model=args.eval_model,
        agent_timeout=args.agent_timeout,
        random_seed=args.seed,
        debug=args.debug,
    )


def _build_delete_config(args: argparse.Namespace) -> RunConfig:
    return RunConfig(
        workspace_root=args.workspace_root.resolve(),
        agent_timeout=args.agent_timeout,
        debug=args.debug,
    )


def _normalize_solution_names(raw_values: list[str]) -> list[str]:
    names: list[str] = []
    for raw_value in raw_values:
        parts = [part.strip() for part in raw_value.split(",")]
        names.extend(part for part in parts if part)
    if len(names) < 2:
        raise ValueError("eval requires at least two solution names")
    return names


def _resolve_agent_source(raw_value: str, *, cwd: Path) -> DockerSolverAgentSource:
    value = raw_value.strip()
    if not value:
        raise ValueError("--agent cannot be empty")

    candidate_path = Path(value).expanduser()
    if candidate_path.exists():
        resolved = _resolve_local_agent_dir(candidate_path.resolve())
        return DockerSolverAgentSource(
            raw=value,
            kind="local_path",
            local_path=str(resolved),
        )

    if candidate_path.is_absolute():
        raise ValueError(f"--agent local path does not exist: {candidate_path}")

    relative_candidate = (cwd / candidate_path).resolve()
    if relative_candidate.exists():
        resolved = _resolve_local_agent_dir(relative_candidate)
        return DockerSolverAgentSource(
            raw=value,
            kind="local_path",
            local_path=str(resolved),
        )

    repo_url, agent_subdir = _normalize_github_agent_source(value)
    if repo_url is None:
        raise ValueError(
            "--agent must be an existing directory or a GitHub repo URL/shorthand like "
            "'github.com/org/repo' or 'https://github.com/org/repo'"
        )

    return DockerSolverAgentSource(
        raw=value,
        kind="github_repo",
        repo_url=repo_url,
        agent_subdir=agent_subdir,
    )


def _normalize_github_agent_source(raw_value: str) -> tuple[str | None, str]:
    cleaned = raw_value.strip().rstrip("/")
    parsed = urlparse(cleaned if "://" in cleaned else f"https://{cleaned}")
    if parsed.netloc.lower() != "github.com":
        return None, "agent"

    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        return None, "agent"

    repo_parts = parts[:2]
    if len(parts) >= 3 and parts[2] == "agent":
        repo_path = "/".join(repo_parts)
        return f"https://github.com/{repo_path}.git", "agent"

    repo_path = "/".join(repo_parts)
    return f"https://github.com/{repo_path}.git", "agent"


def _resolve_local_agent_dir(candidate_dir: Path) -> Path:
    if not candidate_dir.is_dir():
        raise ValueError(f"--agent local path must be a directory: {candidate_dir}")
    if (candidate_dir / "packages" / "coding-agent").is_dir():
        return candidate_dir

    nested_agent_dir = candidate_dir / "agent"
    if (nested_agent_dir / "packages" / "coding-agent").is_dir():
        return nested_agent_dir

    raise ValueError(
        "--agent local path must point to an agent workspace, or to a repo root containing an 'agent/' workspace"
    )


def _load_dotenv() -> None:
    dotenv_path = Path(__file__).resolve().parents[2] / ".env"
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]

        os.environ.setdefault(key, value)


if __name__ == "__main__":
    main()
