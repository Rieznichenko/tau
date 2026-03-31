from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from pathlib import Path

from config import RunConfig
from docker_solver import solve_task_in_docker
from eval import evaluate_candidate_pair
from github_miner import GitHubMiner
from solver_runner import solve_task
from task_generation import generate_task_description
from workspace import (
    delete_all_task_workspaces,
    delete_task_workspace,
    derive_eval_name,
    load_commit_candidate,
    load_generated_task,
    materialize_task_workspace,
    prepare_eval_workspace,
    prepare_solution_workspace,
    resolve_solution_paths,
    resolve_task_paths,
    write_json,
)

log = logging.getLogger("swe-eval")


@dataclass(slots=True)
class GenerateStageResult:
    task_name: str
    repo: str
    commit_sha: str
    task_root: str


@dataclass(slots=True)
class SolveStageResult:
    task_name: str
    solution_name: str
    repo: str
    commit_sha: str
    solution_root: str
    success: bool
    agent: str | None


@dataclass(slots=True)
class EvalStageResult:
    task_name: str
    eval_name: str
    repo: str
    commit_sha: str
    eval_root: str
    comparison_count: int


@dataclass(slots=True)
class DeleteStageResult:
    deleted_count: int
    deleted_tasks: list[str]
    deleted_all: bool


def generate_task_run(*, task_name: str, config: RunConfig) -> GenerateStageResult:
    _setup_logging(debug=config.debug)
    rng = random.Random(config.random_seed)
    miner = GitHubMiner(token=config.github_token, rng=rng, timeout=config.http_timeout)
    try:
        log.debug("Sampling commit candidate for task %s", task_name)
        candidate = miner.sample_commit(max_attempts=config.max_mining_attempts)
    finally:
        miner.close()

    task_paths = materialize_task_workspace(config.tasks_root, task_name, candidate)
    task = generate_task_description(
        candidate=candidate,
        prompt_dir=task_paths.task_dir,
        model=config.generator_model,
        timeout=config.task_generation_timeout,
        openrouter_api_key=config.openrouter_api_key,
    )
    write_json(
        task_paths.task_json_path,
        {
            "stage": "generate",
            "task_name": task_name,
            "repo_full_name": candidate.repo_full_name,
            "commit_sha": candidate.commit_sha,
            "task": task.to_dict(),
        },
    )
    task_paths.task_txt_path.write_text(task.prompt_text + "\n")
    return GenerateStageResult(
        task_name=task_name,
        repo=candidate.repo_full_name,
        commit_sha=candidate.commit_sha,
        task_root=str(task_paths.root),
    )


def solve_task_run(*, task_name: str, solution_name: str, config: RunConfig) -> SolveStageResult:
    _setup_logging(debug=config.debug)
    task_paths = resolve_task_paths(config.tasks_root, task_name)
    candidate = load_commit_candidate(task_paths)
    task = load_generated_task(task_paths)
    solution_paths = prepare_solution_workspace(task_paths, solution_name)

    if config.use_docker_solver:
        solve_result = solve_task_in_docker(
            repo_dir=solution_paths.repo_dir,
            task=task,
            model=config.solver_model,
            timeout=config.agent_timeout,
            config=config,
            run_label=f"{task_name}-{solution_name}",
        )
    else:
        solve_result = solve_task(
            repo_dir=solution_paths.repo_dir,
            task=task,
            model=config.solver_model,
            timeout=config.agent_timeout,
            config=config,
        )

    solution_paths.solution_diff_path.write_text(solve_result.solution_diff + "\n")
    write_json(
        solution_paths.solve_json_path,
        {
            "stage": "solve",
            "task_name": task_name,
            "solution_name": solution_name,
            "repo_full_name": candidate.repo_full_name,
            "commit_sha": candidate.commit_sha,
            "agent": config.docker_solver_agent.raw if config.use_docker_solver and config.docker_solver_agent else None,
            "agent_source": (
                config.docker_solver_agent.to_dict() if config.use_docker_solver and config.docker_solver_agent else None
            ),
            "solver_backend": config.solver_backend,
            "result": solve_result.to_dict(),
        },
    )
    return SolveStageResult(
        task_name=task_name,
        solution_name=solution_name,
        repo=candidate.repo_full_name,
        commit_sha=candidate.commit_sha,
        solution_root=str(solution_paths.root),
        success=solve_result.success,
        agent=config.docker_solver_agent.raw if config.use_docker_solver and config.docker_solver_agent else None,
    )


@dataclass(slots=True)
class ResolvedEvalCandidate:
    name: str
    patch: str
    repo_dir: Path


def evaluate_task_run(*, task_name: str, solution_names: list[str], config: RunConfig) -> EvalStageResult:
    _setup_logging(debug=config.debug)
    task_paths = resolve_task_paths(config.tasks_root, task_name)
    candidate = load_commit_candidate(task_paths)
    task = load_generated_task(task_paths)
    eval_name = derive_eval_name(solution_names)
    eval_paths = prepare_eval_workspace(task_paths, eval_name)
    rng = random.Random(config.random_seed)
    resolved_candidates = [
        _resolve_eval_candidate(task_paths=task_paths, solution_name=solution_name)
        for solution_name in solution_names
    ]
    pairwise_results = []
    for index, (left_candidate, right_candidate) in enumerate(
        zip(resolved_candidates, resolved_candidates[1:], strict=False),
        start=1,
    ):
        comparison_dir = eval_paths.comparisons_dir / f"{index:02d}-{left_candidate.name}-vs-{right_candidate.name}"
        comparison_dir.mkdir(parents=True, exist_ok=False)
        eval_result = evaluate_candidate_pair(
            candidate=candidate,
            task=task,
            reference_patch=task_paths.reference_patch_path.read_text(),
            candidate_a_name=left_candidate.name,
            candidate_b_name=right_candidate.name,
            candidate_a_patch=left_candidate.patch,
            candidate_b_patch=right_candidate.patch,
            workspace_root=comparison_dir,
            original_dir=task_paths.original_dir,
            candidate_a_dir=left_candidate.repo_dir,
            candidate_b_dir=right_candidate.repo_dir,
            prompt_dir=comparison_dir,
            model=config.eval_model,
            timeout=config.agent_timeout,
            rng=rng,
            openrouter_api_key=config.openrouter_api_key,
        )
        pairwise_results.append(
            {
                "index": index,
                "left_solution": left_candidate.name,
                "right_solution": right_candidate.name,
                "winner": eval_result.winner,
                "upstream_winner": eval_result.upstream_winner,
                "rationale": eval_result.rationale,
                "elapsed_seconds": eval_result.elapsed_seconds,
                "model": eval_result.model,
                "candidate_a_label": eval_result.candidate_a_label,
                "candidate_b_label": eval_result.candidate_b_label,
                "prompt_injection_detected": eval_result.prompt_injection_detected,
                "prompt_injection_candidate": eval_result.prompt_injection_candidate,
                "injection_evidence": eval_result.injection_evidence,
                "comparison_root": str(comparison_dir),
            }
        )
    write_json(
        eval_paths.eval_json_path,
        {
            "stage": "eval",
            "task_name": task_name,
            "eval_name": eval_name,
            "solutions": solution_names,
            "repo_full_name": candidate.repo_full_name,
            "commit_sha": candidate.commit_sha,
            "comparisons": pairwise_results,
        },
    )
    return EvalStageResult(
        task_name=task_name,
        eval_name=eval_name,
        repo=candidate.repo_full_name,
        commit_sha=candidate.commit_sha,
        eval_root=str(eval_paths.root),
        comparison_count=len(pairwise_results),
    )


def delete_task_run(*, task_name: str | None, delete_all: bool, config: RunConfig) -> DeleteStageResult:
    _setup_logging(debug=config.debug)
    if delete_all:
        deleted_tasks = delete_all_task_workspaces(config.tasks_root)
        return DeleteStageResult(
            deleted_count=len(deleted_tasks),
            deleted_tasks=deleted_tasks,
            deleted_all=True,
        )

    if not task_name:
        raise ValueError("delete requires --task <name> or --all")

    deleted_task = delete_task_workspace(config.tasks_root, task_name)
    return DeleteStageResult(
        deleted_count=1,
        deleted_tasks=[deleted_task.name],
        deleted_all=False,
    )


def _resolve_eval_candidate(*, task_paths, solution_name: str) -> ResolvedEvalCandidate:
    if solution_name == "original":
        return ResolvedEvalCandidate(
            name=solution_name,
            patch=task_paths.reference_patch_path.read_text(),
            repo_dir=task_paths.reference_dir,
        )

    solution_paths = resolve_solution_paths(task_paths, solution_name)
    return ResolvedEvalCandidate(
        name=solution_name,
        patch=solution_paths.solution_diff_path.read_text(),
        repo_dir=solution_paths.repo_dir,
    )


def _setup_logging(*, debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    root = logging.getLogger()
    root.setLevel(level)

    if not root.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
    else:
        for handler in root.handlers:
            handler.setLevel(level)

    logging.getLogger("httpx").setLevel(logging.INFO if debug else logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
