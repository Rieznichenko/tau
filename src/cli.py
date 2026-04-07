from __future__ import annotations

import argparse
import os
from pathlib import Path
from urllib.parse import urlparse

from config import RunConfig, SolverAgentSource
from pipeline import (
    compare_task_run,
    delete_task_run,
    evaluate_task_run,
    generate_task_run,
    solve_task_run,
)

_DEFAULT_CONCURRENCY = min(os.cpu_count() or 4, 8)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate, solve, compare, and evaluate SWE tasks as independent stages.",
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
        help=(
            "Solver backend selector. Use 'cursor' for the Cursor CLI, 'claude' for the host Claude CLI, "
            "or pass a local agent workspace / repo root / GitHub repo URL for the Docker PI solver."
        ),
    )
    _add_solver_args(solve)

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

    compare = subparsers.add_parser("compare", help="Compare two saved solutions by changed-line similarity.")
    _add_shared_args(compare)
    compare.add_argument("--task", required=True, help="Existing generated task name.")
    compare.add_argument(
        "--solutions",
        required=True,
        nargs="+",
        help="Exactly two solution names to compare. Supports '--solutions A B' and '--solutions A,B'.",
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

    validate = subparsers.add_parser(
        "validate",
        help="Run the live king-of-the-hill validator loop for subnet commitments.",
    )
    _add_shared_args(validate)
    _add_solver_args(validate)
    validate.set_defaults(agent_timeout=1800, docker_solver_max_output_bytes=100000000)
    validate.add_argument("--netuid", type=int, default=66, help="Subnet netuid to validate.")
    validate.add_argument("--network", help="Optional Bittensor network name or websocket endpoint.")
    validate.add_argument(
        "--subtensor-endpoint",
        help="Optional websocket endpoint that overrides --network for chain access.",
    )
    validate.add_argument("--rounds", type=int, default=3, help="Maximum scored rounds per duel (legacy; prefer --max-rounds).")
    validate.add_argument(
        "--concurrency",
        type=int,
        default=_DEFAULT_CONCURRENCY,
        help="Concurrent live tasks per duel (default: min(cpu_count, 8)).",
    )
    validate.add_argument(
        "--epsilon",
        type=float,
        default=0.15,
        help="SPRT epsilon: required win-rate margin above 0.5 to dethrone the king.",
    )
    validate.add_argument(
        "--alpha",
        type=float,
        default=0.05,
        help="SPRT alpha: false-positive rate (probability of wrongly dethroning the king).",
    )
    validate.add_argument(
        "--beta",
        type=float,
        default=0.10,
        help="SPRT beta: false-negative rate (probability of failing to dethrone a better agent).",
    )
    validate.add_argument(
        "--min-rounds",
        type=int,
        default=5,
        help="Minimum scored rounds before SPRT can render a decision.",
    )
    validate.add_argument(
        "--max-rounds",
        type=int,
        default=50,
        help="Safety cap on total scored rounds per duel.",
    )
    validate.add_argument(
        "--copy-similarity-threshold",
        type=float,
        default=0.90,
        help="If mean king-challenger patch similarity exceeds this, disqualify as a copy.",
    )
    validate.add_argument(
        "--max-challengers",
        type=int,
        default=3,
        help="Maximum number of challengers to evaluate simultaneously per epoch.",
    )
    validate.add_argument(
        "--eval-window-seconds",
        type=int,
        default=900,
        help="Maximum wall-clock time to spend on one evaluation epoch.",
    )
    validate.add_argument(
        "--weight-interval-blocks",
        type=int,
        default=360,
        help="How often to set weights to the current king.",
    )
    validate.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=30,
        help="How long to sleep before polling the chain again when idle.",
    )
    validate.add_argument(
        "--queue-size",
        type=int,
        help="Optional maximum number of queued challengers to keep in memory.",
    )
    validate.add_argument(
        "--max-duels",
        type=int,
        help="Optional maximum number of duels to run before exiting.",
    )
    validate.add_argument("--wallet-name", required=True, help="Wallet name to use for set_weights.")
    validate.add_argument("--wallet-hotkey", required=True, help="Wallet hotkey to use for set_weights.")
    validate.add_argument("--wallet-path", help="Optional wallet path override.")
    validate.add_argument(
        "--mock-local-agent",
        help="Optional local agent workspace path to use for offline mock validation submissions.",
    )
    validate.add_argument(
        "--mock-set-weights",
        action="store_true",
        help="Log set_weights calls instead of submitting them on-chain.",
    )
    validate.add_argument(
        "--mock-rounds",
        action="store_true",
        help="Mock validation rounds instead of generating tasks and running solves.",
    )
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
        if args.command == "compare":
            result = compare_task_run(
                task_name=args.task,
                solution_names=_normalize_compare_solution_names(args.solutions),
                config=_build_compare_config(args),
            )
            print(
                f"compared {result.task_name}/{result.comparison_name}: "
                f"{result.repo}@{result.commit_sha[:12]} -> "
                f"{result.matched_changed_lines}/{result.scored_positions} matching changed lines "
                f"({result.similarity_ratio:.2%})"
            )
            print(result.comparison_root)
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
        if args.command == "validate":
            from validate import validate_loop_run

            result = validate_loop_run(config=_build_validate_config(args))
            print(
                f"validate loop exited with king uid={result.king_uid} "
                f"hotkey={result.king_hotkey} repo={result.king_repo}"
            )
            print(result.validate_root)
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
    solver_backend, agent_source = _resolve_solve_target(args.agent, cwd=Path.cwd())
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
        solver_backend=solver_backend,
        solve_agent=args.agent,
        docker_solver_image=args.docker_solver_image,
        solver_agent_source=agent_source,
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


def _build_compare_config(args: argparse.Namespace) -> RunConfig:
    return RunConfig(
        workspace_root=args.workspace_root.resolve(),
        agent_timeout=args.agent_timeout,
        debug=args.debug,
    )


def _build_delete_config(args: argparse.Namespace) -> RunConfig:
    return RunConfig(
        workspace_root=args.workspace_root.resolve(),
        agent_timeout=args.agent_timeout,
        debug=args.debug,
    )


def _build_validate_config(args: argparse.Namespace) -> RunConfig:
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
        docker_solver_image=args.docker_solver_image,
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
        validate_netuid=args.netuid,
        validate_network=args.network,
        validate_subtensor_endpoint=args.subtensor_endpoint,
        validate_rounds=args.rounds,
        validate_concurrency=args.concurrency,
        validate_epsilon=args.epsilon,
        validate_alpha=args.alpha,
        validate_beta=args.beta,
        validate_min_rounds=args.min_rounds,
        validate_max_rounds=args.max_rounds,
        validate_copy_similarity_threshold=args.copy_similarity_threshold,
        validate_max_challengers=args.max_challengers,
        validate_eval_window_seconds=args.eval_window_seconds,
        validate_weight_interval_blocks=args.weight_interval_blocks,
        validate_poll_interval_seconds=args.poll_interval_seconds,
        validate_queue_size=args.queue_size,
        validate_wallet_name=args.wallet_name,
        validate_wallet_hotkey=args.wallet_hotkey,
        validate_wallet_path=args.wallet_path,
        validate_max_duels=args.max_duels,
        validate_mock_local_agent=(
            str(Path(args.mock_local_agent).expanduser().resolve()) if args.mock_local_agent else None
        ),
        validate_mock_set_weights=args.mock_set_weights,
        validate_mock_rounds=args.mock_rounds,
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


def _normalize_compare_solution_names(raw_values: list[str]) -> list[str]:
    names = _normalize_solution_names(raw_values)
    if len(names) != 2:
        raise ValueError("compare requires exactly two solution names")
    return names


def _resolve_solve_target(raw_value: str, *, cwd: Path) -> tuple[str, SolverAgentSource | None]:
    normalized = raw_value.strip().lower()
    if normalized == "cursor":
        return "cursor", None
    if normalized == "claude":
        return "claude", None
    return "docker-pi", _resolve_agent_source(raw_value, cwd=cwd)


def _resolve_agent_source(raw_value: str, *, cwd: Path) -> SolverAgentSource:
    value = raw_value.strip()
    if not value:
        raise ValueError("--agent cannot be empty")

    candidate_path = Path(value).expanduser()
    if candidate_path.exists():
        resolved = _resolve_local_agent_dir(candidate_path.resolve())
        return SolverAgentSource(
            raw=value,
            kind="local_path",
            local_path=str(resolved),
        )

    if candidate_path.is_absolute():
        raise ValueError(f"--agent local path does not exist: {candidate_path}")

    relative_candidate = (cwd / candidate_path).resolve()
    if relative_candidate.exists():
        resolved = _resolve_local_agent_dir(relative_candidate)
        return SolverAgentSource(
            raw=value,
            kind="local_path",
            local_path=str(resolved),
        )

    repo_url, agent_subdir, commit_sha = _normalize_github_agent_source(value)
    if repo_url is None:
        raise ValueError(
            "--agent must be an existing directory or a GitHub repo URL/shorthand like "
            "'github.com/org/repo', 'org/repo@commit', or "
            "'https://github.com/org/repo/commit/<sha>'"
        )

    return SolverAgentSource(
        raw=value,
        kind="github_repo",
        repo_url=repo_url,
        agent_subdir=agent_subdir,
        commit_sha=commit_sha,
    )


def _normalize_github_agent_source(raw_value: str) -> tuple[str | None, str, str | None]:
    cleaned = raw_value.strip().rstrip("/")
    pinned_match = _split_repo_commit_ref(cleaned)
    if pinned_match is not None:
        repo_path, commit_sha = pinned_match
        return f"https://github.com/{repo_path}.git", "agent", commit_sha

    if "://" not in cleaned and cleaned.count("/") >= 1 and not cleaned.startswith("github.com/"):
        parts = [part for part in cleaned.split("/") if part]
        if len(parts) >= 2:
            repo_path = "/".join(parts[:2])
            return f"https://github.com/{repo_path}.git", "agent", None
        return None, "agent", None

    parsed = urlparse(cleaned if "://" in cleaned else f"https://{cleaned}")
    if parsed.netloc.lower() != "github.com":
        return None, "agent", None

    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        return None, "agent", None

    if len(parts) >= 4 and parts[2] == "commit":
        repo_path = "/".join(parts[:2])
        return f"https://github.com/{repo_path}.git", "agent", parts[3]

    repo_parts = parts[:2]
    if len(parts) >= 3 and parts[2] == "agent":
        repo_path = "/".join(repo_parts)
        return f"https://github.com/{repo_path}.git", "agent", None

    repo_path = "/".join(repo_parts)
    return f"https://github.com/{repo_path}.git", "agent", None


def _split_repo_commit_ref(raw_value: str) -> tuple[str, str] | None:
    if "@" not in raw_value or "://" in raw_value or raw_value.startswith("github.com/"):
        return None
    repo_path, commit_sha = raw_value.rsplit("@", 1)
    parts = [part for part in repo_path.split("/") if part]
    if len(parts) != 2 or not commit_sha:
        return None
    return "/".join(parts), commit_sha


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


def _add_solver_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--solver-model", help="Optional model override for solving.")
    parser.add_argument(
        "--seed",
        type=int,
        help="Optional random seed for deterministic solver-side choices.",
    )
    parser.add_argument(
        "--solver-max-requests",
        type=int,
        help="Maximum number of proxied OpenRouter requests allowed for a solve run.",
    )
    parser.add_argument(
        "--solver-max-total-tokens",
        type=int,
        help="Maximum total OpenRouter tokens allowed for a solve run.",
    )
    parser.add_argument(
        "--solver-max-prompt-tokens",
        type=int,
        help="Maximum prompt tokens allowed for a solve run.",
    )
    parser.add_argument(
        "--solver-max-completion-tokens",
        type=int,
        help="Maximum completion tokens allowed for a solve run.",
    )
    parser.add_argument(
        "--solver-max-cost",
        type=float,
        help="Maximum OpenRouter cost allowed for a solve run.",
    )
    parser.add_argument(
        "--solver-max-tokens-per-request",
        type=int,
        help="Maximum completion tokens to allow on any single proxied request.",
    )
    parser.add_argument(
        "--docker-solver-image",
        help="Optional Docker image tag for the solver image. If omitted, one is derived.",
    )
    parser.add_argument(
        "--docker-solver-memory",
        default="2g",
        help="Docker memory limit for the solver container.",
    )
    parser.add_argument(
        "--docker-solver-cpus",
        default="2",
        help="Docker CPU limit for the solver container.",
    )
    parser.add_argument(
        "--docker-solver-pids-limit",
        type=int,
        default=256,
        help="Maximum number of processes allowed inside the solver container.",
    )
    parser.add_argument(
        "--docker-solver-tmp-size",
        default="128m",
        help="Maximum writable size of /tmp inside the solver container.",
    )
    parser.add_argument(
        "--docker-solver-workdir-size",
        default="2g",
        help="Maximum writable size of /work inside the solver container.",
    )
    parser.add_argument(
        "--docker-solver-nofile-limit",
        type=int,
        default=4096,
        help="Maximum number of open files allowed inside the solver container.",
    )
    parser.add_argument(
        "--docker-solver-max-output-bytes",
        type=int,
        default=1000000,
        help="Maximum combined stdout or stderr bytes allowed from the solver command before it is killed.",
    )
    parser.add_argument(
        "--docker-solver-user",
        help="Optional user to run the solver container as.",
    )
    parser.add_argument(
        "--docker-solver-keep-caps",
        action="store_true",
        help="Do not drop Linux capabilities in the solver container.",
    )
    parser.add_argument(
        "--docker-solver-allow-privilege-escalation",
        action="store_true",
        help="Do not set no-new-privileges on the solver container.",
    )
    parser.add_argument(
        "--docker-solver-writeable-rootfs",
        action="store_true",
        help="Do not force the solver container root filesystem to read-only mode.",
    )
    parser.add_argument(
        "--docker-solver-no-cache",
        action="store_true",
        help="Build the solver Docker image with --no-cache.",
    )


def _load_dotenv() -> None:
    dotenv_path = Path(__file__).resolve().parents[1] / ".env"
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
