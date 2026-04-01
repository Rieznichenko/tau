from __future__ import annotations

import hashlib
import json
import logging
import shlex
import subprocess
import tempfile
import textwrap
import time
from pathlib import Path
from typing import Any

from config import RunConfig
from docker_solver import (
    _CONTAINER_REPO_DIR,
    _collect_repo_patch_from_container,
    _container_is_running,
    _copy_directory_to_container,
    _find_repo_symlinks_in_container,
    _kill_container,
    _read_limited_output,
    _remove_container,
    _run,
    _oversized_output_stream,
    _write_text_to_container,
    _apply_patch_to_repo,
)
from solver_runner import (
    COMPLETED_EXIT_REASON,
    SANDBOX_VIOLATION_EXIT_REASON,
    SOLVER_ERROR_EXIT_REASON,
    TIME_LIMIT_EXIT_REASON,
    SolveResult,
    build_solver_prompt,
)
from task_generation import GeneratedTask
from workspace import git_diff

log = logging.getLogger("swe-eval.cursor_runner")

_CURSOR_DOCKERFILE_TEMPLATE = """\
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \\
    bash curl git ca-certificates \\
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.local/bin:/root/.cursor/bin:/usr/local/bin:/usr/bin:/bin:${PATH}"

RUN curl https://cursor.com/install -fsS | bash \\
    && command -v agent >/dev/null

WORKDIR /work

CMD ["bash"]
"""

_CONTAINER_ROOT = "/work"
_CONTAINER_PROMPT_FILE = f"{_CONTAINER_ROOT}/task.txt"
_CURSOR_CLI_PATH = "/root/.local/bin/agent"
_CURSOR_ROLLOUT_FILENAME = "rollout.jsonl"


def solve_task_with_cursor_in_docker(
    *,
    repo_dir: Path,
    task: GeneratedTask,
    model: str | None,
    timeout: int,
    config: RunConfig,
    run_label: str | None = None,
) -> SolveResult:
    if not config.cursor_api_key:
        raise RuntimeError("CURSOR_API_KEY is not set. Export it before running a Cursor solve.")

    prompt = build_solver_prompt(task)
    image_tag = _resolve_image_tag(config)
    start = time.monotonic()
    container_id: str | None = None
    container_force_killed = False
    command_result = _CursorCommandResult(returncode=1, stdout="", stderr="")
    solution_diff = ""

    try:
        _build_image(image_tag=image_tag, config=config)
        container_id = _start_container(image_tag=image_tag, config=config, run_label=run_label)
        _copy_repo_to_container(repo_dir=repo_dir, container_id=container_id)
        _copy_prompt_to_container(prompt=prompt, container_id=container_id)
        command_result = _run_cursor_command(
            container_id=container_id,
            cursor_api_key=config.cursor_api_key,
            model=model,
            timeout=timeout,
            max_output_bytes=config.docker_solver_max_output_bytes,
        )
        if container_id is not None and _container_is_running(container_id):
            symlink_violation = _find_repo_symlinks_in_container(container_id=container_id)
            if symlink_violation:
                command_result.sandbox_violation_reason = f"repository contains symbolic links: {symlink_violation}"
                command_result.stderr = (
                    f"{command_result.stderr}\nCursor solver sandbox violation: {command_result.sandbox_violation_reason}"
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

    elapsed = time.monotonic() - start
    if not solution_diff:
        solution_diff = git_diff(repo_dir)
    exit_reason = _resolve_exit_reason(command_result)
    success = command_result.returncode == 0 and exit_reason == COMPLETED_EXIT_REASON
    raw_output = _build_cursor_raw_output(command_result)
    return SolveResult(
        success=success,
        elapsed_seconds=elapsed,
        raw_output=raw_output,
        model=model,
        solution_diff=solution_diff,
        exit_reason=exit_reason,
        tool_calls=command_result.tool_calls,
        rollout_output=command_result.rollout_output,
        rollout_format="stream-json" if command_result.rollout_output else None,
        rollout_filename=_CURSOR_ROLLOUT_FILENAME if command_result.rollout_output else None,
        session_id=command_result.session_id,
    )


class _CursorCommandResult:
    def __init__(
        self,
        *,
        returncode: int,
        stdout: str,
        stderr: str,
        timed_out: bool = False,
        sandbox_violation_reason: str | None = None,
        parsed_output: str | None = None,
        rollout_output: str | None = None,
        session_id: str | None = None,
        tool_calls: int | None = None,
    ) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.timed_out = timed_out
        self.sandbox_violation_reason = sandbox_violation_reason
        self.parsed_output = parsed_output
        self.rollout_output = rollout_output
        self.session_id = session_id
        self.tool_calls = tool_calls

    @property
    def combined_output(self) -> str:
        return ((self.stdout or "") + (self.stderr or "")).strip()


def _resolve_image_tag(config: RunConfig) -> str:
    if config.docker_solver_image:
        return config.docker_solver_image
    digest = hashlib.sha256()
    digest.update(_CURSOR_DOCKERFILE_TEMPLATE.encode("utf-8"))
    return f"swe-eval/cursor-solver:{digest.hexdigest()[:12]}"


def _build_image(*, image_tag: str, config: RunConfig) -> None:
    inspect_result = _run(["docker", "image", "inspect", image_tag], timeout=30, check=False)
    if inspect_result.returncode == 0 and not config.docker_solver_no_cache:
        log.debug("Reusing existing Cursor solver image %s", image_tag)
        return

    with tempfile.TemporaryDirectory(prefix="swe-eval-cursor-build-") as build_dir:
        build_path = Path(build_dir)
        (build_path / "Dockerfile").write_text(_CURSOR_DOCKERFILE_TEMPLATE)
        cmd = ["docker", "build", "-t", image_tag]
        if config.docker_solver_no_cache:
            cmd.append("--no-cache")
        cmd.append(".")
        result = _run(cmd, cwd=build_path, timeout=1800, check=False)
        if result.returncode != 0:
            output = ((result.stdout or "") + (result.stderr or "")).strip()
            raise RuntimeError(f"Cursor solver image build failed: {output[-500:]}")


def _start_container(*, image_tag: str, config: RunConfig, run_label: str | None) -> str:
    name = _container_name(image_tag, run_label=run_label)
    cmd = [
        "docker",
        "run",
        "-d",
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
        raise RuntimeError(f"Failed to start Cursor solver container: {output[-500:]}")
    return result.stdout.strip()


def _container_name(image_tag: str, *, run_label: str | None) -> str:
    digest = hashlib.sha256(image_tag.encode("utf-8")).hexdigest()[:12]
    label = hashlib.sha256((run_label or str(time.time_ns())).encode("utf-8")).hexdigest()[:10]
    return f"swe-eval-cursor-{digest}-{label}"


def _copy_repo_to_container(*, repo_dir: Path, container_id: str) -> None:
    _run(
        ["docker", "exec", container_id, "bash", "-lc", f"rm -rf {_CONTAINER_REPO_DIR} && mkdir -p {_CONTAINER_REPO_DIR}"],
        timeout=30,
    )
    _copy_directory_to_container(source_dir=repo_dir, container_id=container_id, target_dir=_CONTAINER_REPO_DIR)


def _copy_prompt_to_container(*, prompt: str, container_id: str) -> None:
    _write_text_to_container(
        container_id=container_id,
        target_path=_CONTAINER_PROMPT_FILE,
        content=prompt.rstrip("\n") + "\n",
    )


def _run_cursor_command(
    *,
    container_id: str,
    cursor_api_key: str,
    model: str | None,
    timeout: int,
    max_output_bytes: int,
) -> _CursorCommandResult:
    env_cmd = [
        "docker",
        "exec",
        "-e",
        f"CURSOR_API_KEY={cursor_api_key}",
        "-e",
        "HOME=/tmp/cursor-home",
        container_id,
        "bash",
        "-lc",
        _build_cursor_command(model=model),
    ]
    start = time.monotonic()
    with tempfile.NamedTemporaryFile("w+", prefix="swe-eval-cursor-stdout-", encoding="utf-8") as stdout_file, tempfile.NamedTemporaryFile(
        "w+",
        prefix="swe-eval-cursor-stderr-",
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

        timed_out = False
        sandbox_violation_reason: str | None = None
        while process.poll() is None:
            if time.monotonic() - start > timeout:
                timed_out = True
                _kill_container(container_id)
            else:
                oversized_stream = _oversized_output_stream(
                    stdout_path=Path(stdout_file.name),
                    stderr_path=Path(stderr_file.name),
                    max_output_bytes=max_output_bytes,
                )
                if oversized_stream is not None:
                    sandbox_violation_reason = f"{oversized_stream} exceeded output limit"
                    _kill_container(container_id)
            time.sleep(0.2)

        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)

        stdout = _read_limited_output(Path(stdout_file.name))
        stderr = _read_limited_output(Path(stderr_file.name))
        if timed_out:
            stderr = f"{stderr}\nCursor solver timed out after {timeout}s".strip()
        if sandbox_violation_reason:
            stderr = f"{stderr}\nCursor solver sandbox violation: {sandbox_violation_reason}".strip()
        parsed_output, session_id, tool_calls = _parse_cursor_stream_output(stdout)
        return _CursorCommandResult(
            returncode=process.returncode or 0,
            stdout=stdout,
            stderr=stderr,
            timed_out=timed_out,
            sandbox_violation_reason=sandbox_violation_reason,
            parsed_output=parsed_output,
            rollout_output=stdout.strip() or None,
            session_id=session_id,
            tool_calls=tool_calls,
        )


def _build_cursor_command(*, model: str | None) -> str:
    model_args = f" --model {shlex.quote(model)}" if model else ""
    return textwrap.dedent(
        f"""\
        mkdir -p "$HOME" && \
        PROMPT="$(cat {shlex.quote(_CONTAINER_PROMPT_FILE)})" && \
        cd "$HOME" && \
        {shlex.quote(_CURSOR_CLI_PATH)} -p --force --trust --sandbox disabled --output-format stream-json{model_args} \
            --workspace {shlex.quote(_CONTAINER_REPO_DIR)} "$PROMPT"
        """
    ).strip()


def _build_cursor_raw_output(command_result: _CursorCommandResult) -> str:
    parts: list[str] = []
    if command_result.parsed_output:
        parts.append(command_result.parsed_output.strip())
    if command_result.stderr:
        parts.append(command_result.stderr.strip())
    if parts:
        return "\n\n".join(part for part in parts if part)
    return command_result.combined_output


def _parse_cursor_stream_output(raw_output: str) -> tuple[str, str | None, int | None]:
    text = raw_output.strip()
    if not text:
        return "", None, None

    assistant_messages: list[str] = []
    final_result = ""
    session_id: str | None = None
    tool_call_count = 0

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue

        payload_session_id = payload.get("session_id")
        if session_id is None and isinstance(payload_session_id, str):
            session_id = payload_session_id

        event_type = payload.get("type")
        if event_type == "assistant":
            message_text = _extract_cursor_message_text(payload.get("message"))
            if message_text:
                assistant_messages.append(message_text)
        elif event_type == "result":
            result_text = payload.get("result")
            if isinstance(result_text, str):
                final_result = result_text.strip()
        elif event_type == "tool_call" and payload.get("subtype") == "started":
            tool_call_count += 1

    parsed_output = final_result or "\n\n".join(message for message in assistant_messages if message).strip()
    return parsed_output, session_id, tool_call_count or None


def _extract_cursor_message_text(message: Any) -> str:
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


def _resolve_exit_reason(command_result: _CursorCommandResult) -> str:
    if command_result.timed_out:
        return TIME_LIMIT_EXIT_REASON
    if command_result.sandbox_violation_reason:
        return SANDBOX_VIOLATION_EXIT_REASON
    if command_result.returncode == 0:
        return COMPLETED_EXIT_REASON
    return SOLVER_ERROR_EXIT_REASON
