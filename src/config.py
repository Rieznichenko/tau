from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class SolverAgentSource:
    raw: str
    kind: str
    local_path: str | None = None
    repo_url: str | None = None
    agent_subdir: str | None = None
    commit_sha: str | None = None

    def to_dict(self) -> dict[str, str]:
        payload = {
            "raw": self.raw,
            "kind": self.kind,
        }
        if self.local_path:
            payload["local_path"] = self.local_path
        if self.repo_url:
            payload["repo_url"] = self.repo_url
        if self.agent_subdir:
            payload["agent_subdir"] = self.agent_subdir
        if self.commit_sha:
            payload["commit_sha"] = self.commit_sha
        return payload


@dataclass(slots=True)
class RunConfig:
    """Runtime configuration for staged SWE commands."""

    workspace_root: Path = field(default_factory=Path.cwd)
    github_token: str | None = field(
        default_factory=lambda: os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN"),
    )
    openrouter_api_key: str | None = field(default_factory=lambda: os.environ.get("OPENROUTER_API_KEY"))
    cursor_api_key: str | None = field(default_factory=lambda: os.environ.get("CURSOR_API_KEY"))
    generator_model: str | None = None
    solver_model: str | None = None
    eval_model: str | None = None
    agent_timeout: int = 600
    solver_max_requests: int | None = None
    solver_max_total_tokens: int | None = None
    solver_max_prompt_tokens: int | None = None
    solver_max_completion_tokens: int | None = None
    solver_max_cost: float | None = None
    solver_max_tokens_per_request: int | None = None
    random_seed: int | None = None
    max_mining_attempts: int = 50
    http_timeout: float = 30.0
    solver_backend: str = "claude"
    solve_agent: str | None = None
    docker_solver_image: str | None = None
    solver_agent_source: SolverAgentSource | None = None
    docker_solver_memory: str = "2g"
    docker_solver_cpus: str = "2"
    docker_solver_pids_limit: int = 256
    docker_solver_tmp_size: str = "128m"
    docker_solver_workdir_size: str = "2g"
    docker_solver_nofile_limit: int = 4096
    docker_solver_max_output_bytes: int = 1_000_000
    docker_solver_drop_caps: bool = True
    docker_solver_no_new_privileges: bool = True
    docker_solver_read_only_rootfs: bool = True
    docker_solver_user: str | None = None
    docker_solver_no_cache: bool = False
    validate_netuid: int = 66
    validate_network: str | None = None
    validate_subtensor_endpoint: str | None = None
    validate_rounds: int = 3
    validate_concurrency: int = field(default_factory=lambda: min(os.cpu_count() or 4, 8))
    validate_epsilon: float = 0.15
    validate_alpha: float = 0.05
    validate_beta: float = 0.10
    validate_min_rounds: int = 5
    validate_max_rounds: int = 50
    validate_copy_similarity_threshold: float = 0.90
    validate_max_challengers: int = 3
    validate_eval_window_seconds: int = 900
    validate_weight_interval_blocks: int = 360
    validate_poll_interval_seconds: int = 30
    validate_queue_size: int | None = None
    validate_wallet_name: str | None = None
    validate_wallet_hotkey: str | None = None
    validate_wallet_path: str | None = None
    validate_max_duels: int | None = None
    validate_mock_local_agent: str | None = None
    validate_mock_set_weights: bool = False
    validate_mock_rounds: bool = False
    debug: bool = False

    @property
    def tasks_root(self) -> Path:
        return self.workspace_root / "workspace" / "tasks"

    @property
    def task_generation_timeout(self) -> int:
        return max(self.agent_timeout, 300)

    @property
    def validate_root(self) -> Path:
        return self.workspace_root / "workspace" / "validate" / f"netuid-{self.validate_netuid}"

    @property
    def use_docker_solver(self) -> bool:
        return self.solver_backend == "docker-pi"

    @property
    def use_cursor_solver(self) -> bool:
        return self.solver_backend == "cursor"

    @property
    def use_claude_solver(self) -> bool:
        return self.solver_backend == "claude"
