from __future__ import annotations

import hashlib
import json
import logging
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config import RunConfig
from openrouter_proxy import OpenRouterProxy, SolveBudget
from solver_runner import (
    COMPLETED_EXIT_REASON,
    SANDBOX_VIOLATION_EXIT_REASON,
    SOLVER_ERROR_EXIT_REASON,
    TIME_LIMIT_EXIT_REASON,
    SolveResult,
    build_solver_prompt,
)
from task_generation import GeneratedTask
from workspace import ensure_tree_has_no_symlinks, git_diff

log = logging.getLogger("swe-eval.docker_solver")

_DOCKERFILE_TEMPLATE = """\
FROM node:20-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \\
    bash git ca-certificates python3 make g++ \\
    && rm -rf /var/lib/apt/lists/*

COPY pi-mono-base /opt/pi-mono-base

WORKDIR /opt/pi-mono-base

RUN npm ci \\
    && npm run build --workspace packages/tui --workspace packages/ai --workspace packages/agent --workspace packages/coding-agent \\
    && npm cache clean --force

WORKDIR /work

CMD ["bash"]
"""

_CONTAINER_ROOT = "/work"
_CONTAINER_REPO_DIR = f"{_CONTAINER_ROOT}/repo"
_CONTAINER_AGENT_DIR = f"{_CONTAINER_ROOT}/agent-src"
_CONTAINER_PROMPT_FILE = f"{_CONTAINER_ROOT}/task.txt"
_CONTAINER_TAU_CONFIG_DIR = f"{_CONTAINER_ROOT}/tau-config"
_CONTAINER_PROXY_SOCKET_DIR = "/proxy-socket"
_CONTAINER_PROXY_SOCKET_FILE = f"{_CONTAINER_PROXY_SOCKET_DIR}/openrouter-proxy.sock"
_CONTAINER_PROXY_BRIDGE_FILE = f"{_CONTAINER_ROOT}/proxy_bridge.py"
_CONTAINER_PROXY_PORT = 4318
_IMAGE_PI_MONO_BASE_DIR = "/opt/pi-mono-base"
_PROXY_PROVIDER_NAME = "docker-proxy"
_PROXY_MODEL_NAME = "docker-proxy-model"
_DEFAULT_OPENROUTER_MODEL = "anthropic/claude-sonnet-4"
_SHARED_DOCKER_TEMP_ROOT = Path.home() / ".cache" / "swe-eval"


@dataclass(slots=True)
class _DockerSolverCommandResult:
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool = False
    killed_for_budget: bool = False
    sandbox_violation_reason: str | None = None
    parsed_output: str | None = None
    rollout_output: str | None = None
    session_id: str | None = None
    tool_calls: int | None = None

    @property
    def combined_output(self) -> str:
        return ((self.stdout or "") + (self.stderr or "")).strip()


@dataclass(slots=True)
class _DockerProxyTransport:
    bind_host: str | None
    unix_socket_path: str | None
    container_network: str
    mount_socket_dir: bool
    container_host_name: str | None = None
    relay_container_name: str | None = None
    relay_network_name: str | None = None
    fixed_container_port: bool = False

    def container_base_url(self, proxy: OpenRouterProxy) -> str:
        if self.mount_socket_dir:
            return f"http://127.0.0.1:{_CONTAINER_PROXY_PORT}/v1"
        if not self.container_host_name:
            raise RuntimeError("Container proxy host name is not configured")
        if self.fixed_container_port:
            return f"http://{self.container_host_name}:{_CONTAINER_PROXY_PORT}/v1"
        return proxy.container_base_url(self.container_host_name)


def solve_task_in_docker(
    *,
    repo_dir: Path,
    task: GeneratedTask,
    model: str | None,
    timeout: int,
    config: RunConfig,
    run_label: str | None = None,
) -> SolveResult:
    if not config.openrouter_api_key:
        raise RuntimeError("OPENROUTER_API_KEY is not set. Load it from .env or export it before running swe-eval.")

    prompt = build_solver_prompt(task)
    image_tag = _resolve_image_tag(config)
    model_id = _solver_model_id(config.solver_model)
    log.debug("Prepared Docker tau solver prompt for task %r", task.title)

    start = time.monotonic()
    container_id: str | None = None
    container_force_killed = False
    solver_run = _DockerSolverCommandResult(returncode=1, stdout="", stderr="")
    solution_diff = ""
    budget = SolveBudget.from_config(config)
    bundled_agent_dir = _bundled_agent_dir()
    with tempfile.TemporaryDirectory(prefix="swe-eval-agent-src-") as agent_src_dir, tempfile.TemporaryDirectory(
        prefix="swe-eval-proxy-socket-",
        dir=_shared_docker_temp_root(),
    ) as proxy_socket_dir:
        agent_src_path = Path(agent_src_dir)
        agent_dir = _materialize_agent_source(config=config, target_dir=agent_src_path)
        proxy_transport = _resolve_proxy_transport(proxy_socket_dir=Path(proxy_socket_dir))
        with OpenRouterProxy(
            openrouter_api_key=config.openrouter_api_key,
            solve_budget=budget,
            bind_host=proxy_transport.bind_host,
            unix_socket_path=proxy_transport.unix_socket_path,
            enforced_model=model_id,
            require_auth=True,
        ) as proxy:
            try:
                if proxy_transport.relay_network_name:
                    _create_proxy_relay_network(network_name=proxy_transport.relay_network_name)
                if proxy_transport.relay_container_name:
                    _start_proxy_relay_container(proxy_transport=proxy_transport, proxy=proxy)
                _build_image(image_tag=image_tag, config=config, bundled_agent_dir=bundled_agent_dir)
                container_id = _start_container(
                    image_tag=image_tag,
                    config=config,
                    run_label=run_label,
                    proxy_transport=proxy_transport,
                    proxy_socket_dir=Path(proxy_socket_dir),
                )
                _copy_repo_to_container(repo_dir=repo_dir, container_id=container_id)
                _seed_agent_workspace(container_id=container_id)
                _copy_agent_source_to_container(agent_src_dir=agent_dir, container_id=container_id)
                _copy_prompt_to_container(prompt=prompt, container_id=container_id)
                _copy_tau_config_to_container(
                    container_id=container_id,
                    proxy_base_url=proxy_transport.container_base_url(proxy),
                    model_id=model_id,
                    proxy_auth_token=proxy.auth_token,
                )
                solver_run = _run_solver_command(
                    container_id=container_id,
                    proxy=proxy,
                    timeout=timeout,
                    max_output_bytes=config.docker_solver_max_output_bytes,
                    use_proxy_bridge=proxy_transport.mount_socket_dir,
                )
                if container_id is not None and _container_is_running(container_id):
                    symlink_violation = _find_repo_symlinks_in_container(container_id=container_id)
                    if symlink_violation:
                        solver_run.sandbox_violation_reason = (
                            f"repository contains symbolic links: {symlink_violation}"
                        )
                        solver_run.stderr = (
                            f"{solver_run.stderr}\nDocker tau solver sandbox violation: "
                            f"{solver_run.sandbox_violation_reason}"
                        ).strip()
                        _kill_container(container_id)
                        container_force_killed = True
                    else:
                        solution_diff = _collect_repo_patch_from_container(container_id=container_id)
                        _kill_container(container_id)
                        container_force_killed = True
                        _apply_patch_to_repo(repo_dir=repo_dir, patch_text=solution_diff)
            finally:
                if container_id is not None:
                    if not container_force_killed:
                        _kill_container(container_id)
                    _remove_container(container_id)
                if proxy_transport.relay_container_name:
                    _remove_container(proxy_transport.relay_container_name)
                if proxy_transport.relay_network_name:
                    _remove_network(proxy_transport.relay_network_name)

    elapsed = time.monotonic() - start
    if not solution_diff:
        solution_diff = git_diff(repo_dir)
    usage_summary = proxy.usage_snapshot()
    exit_reason = _resolve_exit_reason(solver_run=solver_run, proxy=proxy)
    success = solver_run.returncode == 0 and exit_reason == COMPLETED_EXIT_REASON
    return SolveResult(
        success=success,
        elapsed_seconds=elapsed,
        raw_output=_build_solver_raw_output(solver_run),
        model=model,
        solution_diff=solution_diff,
        exit_reason=exit_reason,
        usage_summary=usage_summary,
        request_count=usage_summary.request_count,
        prompt_tokens=usage_summary.prompt_tokens,
        completion_tokens=usage_summary.completion_tokens,
        total_tokens=usage_summary.total_tokens,
        cached_tokens=usage_summary.cached_tokens,
        cache_write_tokens=usage_summary.cache_write_tokens,
        reasoning_tokens=usage_summary.reasoning_tokens,
        cost=usage_summary.cost,
        tool_calls=solver_run.tool_calls,
        rollout_output=solver_run.rollout_output,
        rollout_format="pi-json" if solver_run.rollout_output else None,
        rollout_filename="rollout.jsonl" if solver_run.rollout_output else None,
        session_id=solver_run.session_id,
    )


def _build_image(*, image_tag: str, config: RunConfig, bundled_agent_dir: Path) -> None:
    inspect_result = _run(
        ["docker", "image", "inspect", image_tag],
        timeout=30,
        check=False,
    )
    if inspect_result.returncode == 0 and not config.docker_solver_no_cache:
        log.debug("Reusing existing Docker solver image %s", image_tag)
        return

    with tempfile.TemporaryDirectory(prefix="swe-eval-docker-build-") as build_dir:
        build_path = Path(build_dir)
        dockerfile = _DOCKERFILE_TEMPLATE
        (build_path / "Dockerfile").write_text(dockerfile)
        _copy_bundled_agent_to_build_context(bundled_agent_dir=bundled_agent_dir, build_path=build_path)

        cmd = ["docker", "build", "-t", image_tag]
        if config.docker_solver_no_cache:
            cmd.append("--no-cache")
        cmd.append(".")
        result = _run(cmd, cwd=build_path, timeout=1800, check=False)
        if result.returncode != 0:
            output = ((result.stdout or "") + (result.stderr or "")).strip()
            raise RuntimeError(f"Docker solver image build failed: {output[-500:]}")


def _start_container(
    *,
    image_tag: str,
    config: RunConfig,
    run_label: str | None,
    proxy_transport: _DockerProxyTransport,
    proxy_socket_dir: Path,
) -> str:
    name = _container_name(image_tag, run_label=run_label)
    cmd = [
        "docker",
        "run",
        "-d",
        "--network",
        proxy_transport.container_network,
        "--memory",
        config.docker_solver_memory,
        "--memory-swap",
        config.docker_solver_memory,
        "--cpus",
        config.docker_solver_cpus,
        "--pids-limit",
        str(config.docker_solver_pids_limit),
        "--tmpfs",
        f"/tmp:exec,mode=1777,size={config.docker_solver_tmp_size}",
        "--ulimit",
        f"nofile={config.docker_solver_nofile_limit}:{config.docker_solver_nofile_limit}",
        "--name",
        name,
    ]
    if proxy_transport.mount_socket_dir:
        cmd.extend(["--mount", f"type=bind,src={proxy_socket_dir},dst={_CONTAINER_PROXY_SOCKET_DIR},readonly"])
    if config.docker_solver_drop_caps:
        cmd.extend(["--cap-drop", "ALL"])
    if config.docker_solver_no_new_privileges:
        cmd.extend(["--security-opt", "no-new-privileges:true"])
    if config.docker_solver_read_only_rootfs:
        cmd.append("--read-only")
        cmd.extend(["--tmpfs", f"/work:exec,mode=1777,size={config.docker_solver_workdir_size}"])
    if config.docker_solver_user:
        cmd.extend(["--user", config.docker_solver_user])
    cmd.extend([image_tag, "sleep", "3600"])
    result = _run(cmd, timeout=30, check=False)
    if result.returncode != 0:
        output = ((result.stdout or "") + (result.stderr or "")).strip()
        raise RuntimeError(f"Failed to start Docker solver container: {output[-500:]}")
    return result.stdout.strip()


def _copy_repo_to_container(*, repo_dir: Path, container_id: str) -> None:
    _run(
        ["docker", "exec", container_id, "bash", "-lc", f"rm -rf {_CONTAINER_REPO_DIR} && mkdir -p {_CONTAINER_REPO_DIR}"],
        timeout=30,
    )
    _copy_directory_to_container(source_dir=repo_dir, container_id=container_id, target_dir=_CONTAINER_REPO_DIR)


def _copy_agent_source_to_container(*, agent_src_dir: Path, container_id: str) -> None:
    _copy_directory_to_container(
        source_dir=agent_src_dir,
        container_id=container_id,
        target_dir=_CONTAINER_AGENT_DIR,
        exclude_names={".git", "node_modules", "dist", ".DS_Store"},
    )


def _seed_agent_workspace(*, container_id: str) -> None:
    _run(
        [
            "docker",
            "exec",
            container_id,
            "bash",
            "-lc",
            (
                f'rm -rf "{_CONTAINER_AGENT_DIR}" && mkdir -p "{_CONTAINER_AGENT_DIR}" && '
                f'cp -R "{_IMAGE_PI_MONO_BASE_DIR}/." "{_CONTAINER_AGENT_DIR}"'
            ),
        ],
        timeout=30,
    )


def _copy_prompt_to_container(*, prompt: str, container_id: str) -> None:
    _write_text_to_container(
        container_id=container_id,
        target_path=_CONTAINER_PROMPT_FILE,
        content=prompt.rstrip("\n") + "\n",
    )


def _copy_proxy_bridge_script(*, container_id: str) -> None:
    _write_text_to_container(
        container_id=container_id,
        target_path=_CONTAINER_PROXY_BRIDGE_FILE,
        content=_proxy_bridge_script() + "\n",
    )


def _copy_tau_config_to_container(
    *,
    container_id: str,
    proxy_base_url: str,
    model_id: str,
    proxy_auth_token: str,
) -> None:
    config_payload = {
        "providers": {
            _PROXY_PROVIDER_NAME: {
                "baseUrl": proxy_base_url,
                "api": "openai-completions",
                "apiKey": proxy_auth_token,
                "authHeader": True,
                "models": [
                    {
                        "id": model_id,
                        "name": _PROXY_MODEL_NAME,
                        "reasoning": False,
                    },
                ],
            },
        },
    }

    _write_text_to_container(
        container_id=container_id,
        target_path=f"{_CONTAINER_TAU_CONFIG_DIR}/models.json",
        content=json.dumps(config_payload, indent=2) + "\n",
    )


def _copy_directory_to_container(
    *,
    source_dir: Path,
    container_id: str,
    target_dir: str,
    exclude_names: set[str] | None = None,
) -> None:
    if not source_dir.is_dir():
        raise RuntimeError(f"Directory to copy does not exist: {source_dir}")

    extract_script = textwrap.dedent(
        """\
        import sys
        import tarfile
        from pathlib import Path

        target = Path(sys.argv[1])
        target.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=sys.stdin.buffer, mode="r|*") as archive:
            archive.extractall(target)
        """,
    ).strip()

    with tempfile.NamedTemporaryFile(suffix=".tar") as tar_file:
        with tarfile.open(fileobj=tar_file, mode="w") as archive:
            archive.add(source_dir, arcname=".", filter=_tar_filter(exclude_names))
        tar_file.flush()
        tar_file.seek(0)
        result = subprocess.run(
            ["docker", "exec", "-i", container_id, "python3", "-c", extract_script, target_dir],
            stdin=tar_file,
            capture_output=True,
            text=True,
            timeout=300,
        )
    if result.returncode != 0:
        output = ((result.stdout or "") + (result.stderr or "")).strip()
        raise RuntimeError(f"Failed to copy directory into container: {output[-500:]}")


def _tar_filter(exclude_names: set[str] | None):
    def filter_member(tar_info: tarfile.TarInfo) -> tarfile.TarInfo | None:
        if not exclude_names:
            return tar_info
        parts = [part for part in Path(tar_info.name).parts if part not in {".", ""}]
        if any(part in exclude_names for part in parts):
            return None
        return tar_info

    return filter_member


def _write_text_to_container(*, container_id: str, target_path: str, content: str) -> None:
    parent_dir = str(Path(target_path).parent)
    quoted_parent_dir = shlex.quote(parent_dir)
    _run(
        ["docker", "exec", container_id, "bash", "-lc", f"mkdir -p {quoted_parent_dir}"],
        timeout=30,
    )
    write_script = textwrap.dedent(
        """\
        import sys
        from pathlib import Path

        target = Path(sys.argv[1])
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(sys.stdin.read())
        """,
    ).strip()
    result = subprocess.run(
        ["docker", "exec", "-i", container_id, "python3", "-c", write_script, target_path],
        input=content,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        output = ((result.stdout or "") + (result.stderr or "")).strip()
        raise RuntimeError(f"Failed to write file into container: {output[-500:]}")


def _run_solver_command(
    *,
    container_id: str,
    proxy: OpenRouterProxy,
    timeout: int,
    max_output_bytes: int,
    use_proxy_bridge: bool,
) -> _DockerSolverCommandResult:
    prompt_cmd = _build_solver_command(
        use_proxy_bridge=use_proxy_bridge,
    )
    env_cmd = [
        "docker",
        "exec",
        "-e",
        f"TAU_CODING_AGENT_DIR={_CONTAINER_TAU_CONFIG_DIR}",
        "-e",
        f"PI_CODING_AGENT_DIR={_CONTAINER_TAU_CONFIG_DIR}",
        "-e",
        f"TAU_AGENT_DIR={_CONTAINER_AGENT_DIR}",
        "-e",
        f"PI_AGENT_DIR={_CONTAINER_AGENT_DIR}",
        "-e",
        f"TAU_REPO_DIR={_CONTAINER_REPO_DIR}",
        "-e",
        f"PI_REPO_DIR={_CONTAINER_REPO_DIR}",
        "-e",
        f"TAU_PROMPT_FILE={_CONTAINER_PROMPT_FILE}",
        "-e",
        f"PI_PROMPT_FILE={_CONTAINER_PROMPT_FILE}",
        container_id,
        "bash",
        "-lc",
        prompt_cmd,
    ]
    if use_proxy_bridge:
        _copy_proxy_bridge_script(container_id=container_id)
        # Insert proxy env vars before the container_id arg
        _cid_idx = env_cmd.index(container_id)
        env_cmd[_cid_idx:_cid_idx] = [
            "-e",
            f"TAU_PROXY_BRIDGE={_CONTAINER_PROXY_BRIDGE_FILE}",
            "-e",
            f"PI_PROXY_BRIDGE={_CONTAINER_PROXY_BRIDGE_FILE}",
            "-e",
            f"TAU_PROXY_SOCKET_PATH={_CONTAINER_PROXY_SOCKET_FILE}",
            "-e",
            f"PI_PROXY_SOCKET_PATH={_CONTAINER_PROXY_SOCKET_FILE}",
            "-e",
            f"TAU_PROXY_LISTEN_PORT={_CONTAINER_PROXY_PORT}",
            "-e",
            f"PI_PROXY_LISTEN_PORT={_CONTAINER_PROXY_PORT}",
        ]
    start = time.monotonic()
    with tempfile.NamedTemporaryFile("w+", prefix="swe-eval-solver-stdout-", encoding="utf-8") as stdout_file, tempfile.NamedTemporaryFile(
        "w+",
        prefix="swe-eval-solver-stderr-",
        encoding="utf-8",
    ) as stderr_file:
        try:
            process = subprocess.Popen(
                env_cmd,
                stdout=stdout_file,
                stderr=stderr_file,
                text=True,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(f"Required command not found: {env_cmd[0]}") from exc

        killed_for_budget = False
        timed_out = False
        sandbox_violation_reason: str | None = None
        while process.poll() is None:
            if proxy.budget_exceeded_reason and not killed_for_budget:
                killed_for_budget = True
                _kill_container(container_id)
            elif time.monotonic() - start > timeout:
                timed_out = True
                _kill_container(container_id)
            time.sleep(0.2)

        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)

        stdout = _read_limited_output(Path(stdout_file.name), max_output_bytes=max_output_bytes)
        stderr = _read_limited_output(Path(stderr_file.name), max_output_bytes=max_output_bytes)

        if timed_out:
            stderr = f"{stderr}\nDocker tau solver timed out after {timeout}s".strip()
        if sandbox_violation_reason:
            stderr = f"{stderr}\nDocker tau solver sandbox violation: {sandbox_violation_reason}".strip()
        parsed_output, rollout_output, session_id, tool_calls = _parse_pi_json_output(stdout)
        return _DockerSolverCommandResult(
            returncode=process.returncode or 0,
            stdout=stdout,
            stderr=stderr,
            timed_out=timed_out,
            killed_for_budget=killed_for_budget,
            sandbox_violation_reason=sandbox_violation_reason,
            parsed_output=parsed_output,
            rollout_output=rollout_output,
            session_id=session_id,
            tool_calls=tool_calls,
        )


def _remove_container(container_id: str) -> None:
    _run(["docker", "rm", "-f", container_id], timeout=30, check=False)


def _kill_container(container_id: str) -> None:
    _run(["docker", "kill", container_id], timeout=30, check=False)


def _resolve_image_tag(config: RunConfig) -> str:
    if config.docker_solver_image:
        return config.docker_solver_image
    digest = hashlib.sha256()
    digest.update(_DOCKERFILE_TEMPLATE.encode("utf-8"))
    digest.update(_hash_directory(_bundled_agent_dir()))
    return f"swe-eval/tau-solver:{digest.hexdigest()[:12]}"


def _container_name(image_tag: str, *, run_label: str | None) -> str:
    digest = hashlib.sha256(image_tag.encode("utf-8")).hexdigest()[:12]
    label = hashlib.sha256((run_label or str(time.time_ns())).encode("utf-8")).hexdigest()[:10]
    return f"swe-eval-tau-{digest}-{label}"


def _bundled_agent_dir() -> Path:
    agent_dir = Path(__file__).resolve().parents[1] / "agent"
    if not agent_dir.is_dir():
        raise RuntimeError(f"Bundled agent workspace is missing: {agent_dir}")
    return agent_dir


def _copy_bundled_agent_to_build_context(*, bundled_agent_dir: Path, build_path: Path) -> None:
    _validate_agent_workspace(bundled_agent_dir)
    source_dir = build_path / "pi-mono-base"
    shutil.copytree(
        bundled_agent_dir,
        source_dir,
        ignore=shutil.ignore_patterns(".git", "node_modules", "dist", ".DS_Store"),
        symlinks=True,
    )


def _hash_directory(root: Path) -> bytes:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        relative_path = path.relative_to(root)
        if any(part in {".git", "node_modules", "dist", ".DS_Store"} for part in relative_path.parts):
            continue
        digest.update(str(relative_path).encode("utf-8"))
        if path.is_file():
            digest.update(path.read_bytes())
    return digest.digest()


def _run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    timeout: int,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    log.debug("Running command: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"Required command not found: {cmd[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"Command timed out after {timeout}s: {' '.join(cmd[:3])}") from exc

    if check and result.returncode != 0:
        output = ((result.stdout or "") + (result.stderr or "")).strip()
        raise RuntimeError(f"Command failed ({' '.join(cmd[:3])}): {output[-500:]}")
    return result


def _build_solver_command(*, use_proxy_bridge: bool) -> str:
    setup_parts = [
        'export PATH="$TAU_AGENT_DIR/node_modules/.bin:$PATH"',
    ]
    prefix = " && ".join(setup_parts)
    if use_proxy_bridge:
        proxy_ready_check = shlex.quote(
            'import os, socket; '
            'sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM); '
            'sock.settimeout(0.1); '
            'sock.connect(("127.0.0.1", int(os.environ["TAU_PROXY_LISTEN_PORT"]))); '
            'sock.close()',
        )
        prefix = " && ".join(
            [
                prefix,
                'python3 "$TAU_PROXY_BRIDGE" & BRIDGE_PID=$!',
                "trap 'kill $BRIDGE_PID >/dev/null 2>&1 || true' EXIT",
                'for _ in $(seq 1 50); do '
                f'python3 -c {proxy_ready_check} && break || sleep 0.1; '
                'done',
            ],
        )
    command_parts = [
        prefix,
        [
            'cd "$TAU_AGENT_DIR"',
            "tsgo -p packages/tui/tsconfig.build.json",
            "tsgo -p packages/ai/tsconfig.build.json",
            "tsgo -p packages/agent/tsconfig.build.json",
            "tsgo -p packages/coding-agent/tsconfig.build.json",
            'shx chmod +x "$TAU_AGENT_DIR/packages/coding-agent/dist/cli.js"',
            "npm --workspace packages/coding-agent run copy-assets",
            'test -f "$TAU_AGENT_DIR/packages/coding-agent/dist/cli.js"',
            'PROMPT="$(cat "$TAU_PROMPT_FILE")"',
            'cd "$TAU_REPO_DIR"',
            (
                'node "$TAU_AGENT_DIR/packages/coding-agent/dist/cli.js" '
                f'--mode json --no-session --provider "{_PROXY_PROVIDER_NAME}" --model "{_PROXY_MODEL_NAME}" -p "$PROMPT"'
            ),
        ],
    ]
    flattened_parts = [command_parts[0], *command_parts[1]]
    return " && ".join(flattened_parts)


def _build_solver_raw_output(solver_run: _DockerSolverCommandResult) -> str:
    parts: list[str] = []
    if solver_run.parsed_output:
        parts.append(solver_run.parsed_output.strip())
    if solver_run.stderr:
        parts.append(solver_run.stderr.strip())
    if parts:
        return "\n\n".join(part for part in parts if part)
    return solver_run.combined_output


def _parse_pi_json_output(raw_output: str) -> tuple[str, str | None, str | None, int | None]:
    if not raw_output.strip():
        return "", None, None, None

    rollout_lines: list[str] = []
    final_output = ""
    text_deltas: list[str] = []
    session_id: str | None = None
    tool_call_count = 0

    for line in raw_output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue

        rollout_lines.append(json.dumps(payload, sort_keys=True))

        event_type = payload.get("type")
        if event_type == "session":
            payload_session_id = payload.get("id")
            if isinstance(payload_session_id, str):
                session_id = payload_session_id
        elif event_type == "message_update":
            assistant_event = payload.get("assistantMessageEvent")
            if isinstance(assistant_event, dict) and assistant_event.get("type") == "text_delta":
                delta = assistant_event.get("delta")
                if isinstance(delta, str):
                    text_deltas.append(delta)
        elif event_type == "turn_end":
            message = payload.get("message")
            message_text = _extract_pi_message_text(message)
            if message_text:
                final_output = message_text
        elif event_type == "tool_execution_start":
            tool_call_count += 1

    if not final_output:
        final_output = "".join(text_deltas).strip()
    rollout_output = "\n".join(rollout_lines).strip() or None
    return final_output, rollout_output, session_id, tool_call_count or None


def _extract_pi_message_text(message: Any) -> str:
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if not isinstance(content, list):
        return ""

    parts: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "text":
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text)
    return "\n".join(parts).strip()


def _resolve_proxy_transport(*, proxy_socket_dir: Path) -> _DockerProxyTransport:
    if sys.platform.startswith("linux"):
        return _DockerProxyTransport(
            bind_host=None,
            unix_socket_path=str(proxy_socket_dir / "openrouter-proxy.sock"),
            container_network="none",
            mount_socket_dir=True,
        )

    token = hashlib.sha256(str(time.time_ns()).encode("utf-8")).hexdigest()[:12]
    relay_name = f"swe-eval-proxy-relay-{token}"
    network_name = f"swe-eval-solver-net-{token}"
    return _DockerProxyTransport(
        bind_host="0.0.0.0",
        unix_socket_path=None,
        container_network=network_name,
        mount_socket_dir=False,
        container_host_name=relay_name,
        relay_container_name=relay_name,
        relay_network_name=network_name,
        fixed_container_port=True,
    )


def _shared_docker_temp_root() -> str:
    # Keep bind-mounted socket paths under the user's home directory so Docker
    # Desktop can mount them without exposing a host TCP listener.
    _SHARED_DOCKER_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    return str(_SHARED_DOCKER_TEMP_ROOT)


def _create_proxy_relay_network(*, network_name: str) -> None:
    _run(["docker", "network", "create", "--internal", network_name], timeout=30)


def _start_proxy_relay_container(*, proxy_transport: _DockerProxyTransport, proxy: OpenRouterProxy) -> None:
    if not proxy_transport.relay_container_name or not proxy_transport.relay_network_name:
        raise RuntimeError("Proxy relay transport is missing relay container metadata")

    relay_target = f"host.docker.internal:{proxy.port}"
    _run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            proxy_transport.relay_container_name,
            "--network",
            "bridge",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges:true",
            "--pids-limit",
            "64",
            "--memory",
            "64m",
            "--memory-swap",
            "64m",
            "--cpus",
            "0.5",
            "alpine/socat:latest",
            "-d",
            "-d",
            f"TCP-LISTEN:{_CONTAINER_PROXY_PORT},fork,reuseaddr,bind=0.0.0.0",
            f"TCP:{relay_target}",
        ],
        timeout=30,
    )
    _run(
        [
            "docker",
            "network",
            "connect",
            "--alias",
            proxy_transport.relay_container_name,
            proxy_transport.relay_network_name,
            proxy_transport.relay_container_name,
        ],
        timeout=30,
    )


def _remove_network(network_name: str) -> None:
    _run(["docker", "network", "rm", network_name], timeout=30, check=False)


def _solver_model_id(model: str | None) -> str:
    if not model:
        return _DEFAULT_OPENROUTER_MODEL
    if model.startswith("openrouter/"):
        return model.split("/", 1)[1]
    return model


def _resolve_exit_reason(*, solver_run: _DockerSolverCommandResult, proxy: OpenRouterProxy) -> str:
    if solver_run.timed_out:
        return TIME_LIMIT_EXIT_REASON
    if solver_run.sandbox_violation_reason:
        return SANDBOX_VIOLATION_EXIT_REASON
    if proxy.budget_exceeded_reason:
        return proxy.budget_exceeded_reason
    if solver_run.returncode == 0:
        return COMPLETED_EXIT_REASON
    return SOLVER_ERROR_EXIT_REASON


def _proxy_bridge_script() -> str:
    return textwrap.dedent(
        """\
        import os
        import socket
        import threading

        LISTEN_HOST = "127.0.0.1"
        LISTEN_PORT = int(os.environ.get("TAU_PROXY_LISTEN_PORT") or os.environ["PI_PROXY_LISTEN_PORT"])
        SOCKET_PATH = os.environ.get("TAU_PROXY_SOCKET_PATH") or os.environ["PI_PROXY_SOCKET_PATH"]


        def _pipe(source, destination):
            try:
                while True:
                    chunk = source.recv(65536)
                    if not chunk:
                        try:
                            destination.shutdown(socket.SHUT_WR)
                        except OSError:
                            pass
                        break
                    destination.sendall(chunk)
            finally:
                try:
                    destination.close()
                except OSError:
                    pass
                try:
                    source.close()
                except OSError:
                    pass


        def _handle(client):
            upstream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            upstream.connect(SOCKET_PATH)
            threading.Thread(target=_pipe, args=(client, upstream), daemon=True).start()
            threading.Thread(target=_pipe, args=(upstream, client), daemon=True).start()


        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((LISTEN_HOST, LISTEN_PORT))
        server.listen()

        while True:
            client, _ = server.accept()
            threading.Thread(target=_handle, args=(client,), daemon=True).start()
        """,
    ).strip()


def _read_limited_output(path: Path, *, max_output_bytes: int | None = None) -> str:
    if not path.exists():
        return ""
    raw_bytes = path.read_bytes()
    if max_output_bytes is not None and len(raw_bytes) > max_output_bytes:
        raw_bytes = raw_bytes[-max_output_bytes:]
    return raw_bytes.decode("utf-8", errors="replace")


def _container_is_running(container_id: str) -> bool:
    result = _run(
        ["docker", "inspect", "-f", "{{.State.Running}}", container_id],
        timeout=30,
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip().lower() == "true"


def _collect_repo_patch_from_container(*, container_id: str) -> str:
    patch_cmd = (
        'cd "$TAU_REPO_DIR" && '
        'git diff --binary && '
        'while IFS= read -r -d \'\' path; do '
        'git diff --binary --no-index -- /dev/null "$path" || test $? -eq 1; '
        'done < <(git ls-files --others --exclude-standard -z)'
    )
    result = _run(
        [
            "docker",
            "exec",
            "-e",
            f"TAU_REPO_DIR={_CONTAINER_REPO_DIR}",
            container_id,
            "bash",
            "-lc",
            patch_cmd,
        ],
        timeout=120,
        check=False,
    )
    if result.returncode not in (0, 1):
        output = ((result.stdout or "") + (result.stderr or "")).strip()
        raise RuntimeError(f"Failed to collect solver patch from container: {output[-500:]}")
    return result.stdout or ""


def _find_repo_symlinks_in_container(*, container_id: str) -> str | None:
    check_script = textwrap.dedent(
        """\
        import os
        from pathlib import Path

        repo_dir = Path(os.environ.get("TAU_REPO_DIR") or os.environ["PI_REPO_DIR"])
        symlinks = []
        for current_root, dirnames, filenames in os.walk(repo_dir, topdown=True, followlinks=False):
            current_dir = Path(current_root)
            for name in [*dirnames, *filenames]:
                candidate = current_dir / name
                if candidate.is_symlink():
                    symlinks.append(str(candidate.relative_to(repo_dir)))

        print("\\n".join(sorted(symlinks[:10])))
        """,
    ).strip()
    result = _run(
        [
            "docker",
            "exec",
            "-e",
            f"TAU_REPO_DIR={_CONTAINER_REPO_DIR}",
            container_id,
            "python3",
            "-c",
            check_script,
        ],
        timeout=120,
    )
    symlinks = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not symlinks:
        return None
    sample = ", ".join(symlinks)
    if len(symlinks) == 10:
        sample = f"{sample}, ..."
    return sample


def _apply_patch_to_repo(*, repo_dir: Path, patch_text: str) -> None:
    if not patch_text.strip():
        return
    with tempfile.NamedTemporaryFile("w", suffix=".patch", delete=False) as temp_file:
        temp_file.write(patch_text)
        temp_file.write("\n")
        temp_path = Path(temp_file.name)
    try:
        _run(
            ["git", "apply", "--binary", "--whitespace=nowarn", str(temp_path)],
            cwd=repo_dir,
            timeout=120,
        )
        ensure_tree_has_no_symlinks(repo_dir, label="solver output tree")
    finally:
        temp_path.unlink(missing_ok=True)


def _validate_agent_workspace(agent_dir: Path) -> Path:
    package_path = agent_dir / "package.json"
    coding_agent_dir = agent_dir / "packages" / "coding-agent"
    if not package_path.is_file():
        raise RuntimeError(f"Agent workspace is missing package.json: {agent_dir}")
    if not coding_agent_dir.is_dir():
        raise RuntimeError(f"Agent workspace is missing packages/coding-agent: {agent_dir}")
    return agent_dir


def _materialize_agent_source(*, config: RunConfig, target_dir: Path) -> Path:
    agent = config.solver_agent_source
    if agent is None:
        raise RuntimeError("Docker solver agent is not configured")

    if agent.kind == "local_path":
        if not agent.local_path:
            raise RuntimeError("Docker solver local agent path is missing")
        agent_dir = Path(agent.local_path).expanduser().resolve()
        if not agent_dir.is_dir():
            raise RuntimeError(f"Local agent path does not exist: {agent_dir}")
        return _validate_agent_workspace(agent_dir)

    if agent.kind == "github_repo":
        if not agent.repo_url:
            raise RuntimeError("Docker solver GitHub repo URL is missing")
        target_dir.mkdir(parents=True, exist_ok=True)
        if agent.commit_sha:
            clone_result = _run(
                ["git", "clone", "--filter=blob:none", "--no-checkout", agent.repo_url, str(target_dir)],
                timeout=300,
                check=False,
            )
            if clone_result.returncode != 0:
                output = ((clone_result.stdout or "") + (clone_result.stderr or "")).strip()
                raise RuntimeError(f"Failed to clone agent repository: {output[-500:]}")

            # Resolve short SHA to full SHA via ls-remote, since git fetch --depth=1
            # requires a full SHA or ref name (short SHAs fail as remote refs).
            commit_ref = agent.commit_sha
            if len(commit_ref) < 40:
                ls_result = _run(
                    ["git", "ls-remote", "origin"],
                    cwd=target_dir,
                    timeout=60,
                    check=False,
                )
                if ls_result.returncode == 0 and ls_result.stdout:
                    for line in ls_result.stdout.strip().splitlines():
                        full_sha = line.split("\t")[0]
                        if full_sha.startswith(commit_ref):
                            commit_ref = full_sha
                            break

            fetch_result = _run(
                ["git", "fetch", "--depth=1", "origin", commit_ref],
                cwd=target_dir,
                timeout=180,
                check=False,
            )
            if fetch_result.returncode != 0:
                output = ((fetch_result.stdout or "") + (fetch_result.stderr or "")).strip()
                raise RuntimeError(f"Failed to fetch pinned agent commit: {output[-500:]}")

            checkout_result = _run(
                ["git", "checkout", "--detach", "FETCH_HEAD"],
                cwd=target_dir,
                timeout=120,
                check=False,
            )
            if checkout_result.returncode != 0:
                output = ((checkout_result.stdout or "") + (checkout_result.stderr or "")).strip()
                raise RuntimeError(f"Failed to checkout pinned agent commit: {output[-500:]}")

            head_result = _run(
                ["git", "rev-parse", "HEAD"],
                cwd=target_dir,
                timeout=30,
                check=False,
            )
            if head_result.returncode != 0:
                output = ((head_result.stdout or "") + (head_result.stderr or "")).strip()
                raise RuntimeError(f"Failed to verify pinned agent commit: {output[-500:]}")

            resolved_head = head_result.stdout.strip()
            if not resolved_head.startswith(agent.commit_sha):
                raise RuntimeError(
                    f"Pinned agent commit mismatch: requested {agent.commit_sha}, got {resolved_head}"
                )
        else:
            clone_result = _run(
                ["git", "clone", "--depth=1", agent.repo_url, str(target_dir)],
                timeout=300,
                check=False,
            )
            if clone_result.returncode != 0:
                output = ((clone_result.stdout or "") + (clone_result.stderr or "")).strip()
                raise RuntimeError(f"Failed to clone agent repository: {output[-500:]}")

        agent_dir = target_dir / (agent.agent_subdir or "agent")
        if not agent_dir.is_dir():
            raise RuntimeError(f"Resolved agent directory does not exist in cloned repo: {agent_dir}")
        return _validate_agent_workspace(agent_dir)

    raise RuntimeError(f"Unsupported docker solver agent kind: {agent.kind}")
