from __future__ import annotations

import logging
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from openrouter_proxy import OpenRouterProxy, SolveBudget, SolveUsageSummary

log = logging.getLogger("swe-eval.claude_runner")


@dataclass(slots=True)
class ClaudeRunResult:
    returncode: int
    stdout: str
    stderr: str
    elapsed_seconds: float
    usage_summary: SolveUsageSummary | None = None
    budget_exceeded_reason: str | None = None
    timed_out: bool = False

    @property
    def combined_output(self) -> str:
        return ((self.stdout or "") + (self.stderr or "")).strip()


def run_claude(
    *,
    prompt: str,
    cwd: Path,
    model: str | None,
    timeout: int,
    output_format: str = "text",
    openrouter_api_key: str | None = None,
    solve_budget: SolveBudget | None = None,
    additional_dirs: list[Path] | None = None,
    permission_mode: str = "bypassPermissions",
    tools: str | None = None,
) -> ClaudeRunResult:
    cmd = [
        "claude",
        "-p",
        "--output-format",
        output_format,
        "--permission-mode",
        permission_mode,
        "--no-session-persistence",
    ]
    if model:
        cmd.extend(["--model", model])
    if tools:
        cmd.extend(["--tools", tools])

    allowed_dirs = _dedupe_paths(additional_dirs or [])
    for directory in allowed_dirs:
        cmd.extend(["--add-dir", str(directory)])
    cmd.append(prompt)

    log.debug(
        "Running claude in %s (model=%s, timeout=%ss, format=%s, permission_mode=%s, tools=%s)",
        cwd,
        model,
        timeout,
        output_format,
        permission_mode,
        tools,
    )
    start = time.monotonic()
    env = os.environ.copy()
    proxy: OpenRouterProxy | None = None
    returncode = 1
    stdout = ""
    stderr = ""
    timed_out = False

    if openrouter_api_key:
        proxy = OpenRouterProxy(
            openrouter_api_key=openrouter_api_key,
            solve_budget=solve_budget,
        )
        proxy.start()
        base_url = f"http://127.0.0.1:{proxy.port}"
        env["ANTHROPIC_BASE_URL"] = base_url
        env["CLAUDE_CODE_API_BASE_URL"] = base_url
        env["ANTHROPIC_API_KEY"] = proxy.auth_token

    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        returncode = result.returncode
        stdout = result.stdout or ""
        stderr = result.stderr or ""
    except FileNotFoundError as exc:
        raise RuntimeError("`claude` CLI not found on PATH") from exc
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        returncode = 124
        stdout = _coerce_process_output(exc.stdout)
        timeout_message = f"Claude timed out after {timeout}s"
        existing_stderr = _coerce_process_output(exc.stderr)
        stderr = f"{existing_stderr}\n{timeout_message}".strip()
    finally:
        elapsed = time.monotonic() - start
        usage_summary = proxy.usage_snapshot() if proxy is not None else None
        budget_exceeded_reason = proxy.budget_exceeded_reason if proxy is not None else None
        if proxy is not None:
            proxy.stop()

    log.debug("Claude exited with code %s in %.2fs", returncode, elapsed)
    return ClaudeRunResult(
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
        elapsed_seconds=elapsed,
        usage_summary=usage_summary,
        budget_exceeded_reason=budget_exceeded_reason,
        timed_out=timed_out,
    )


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(resolved)
    return unique


def _coerce_process_output(raw_output: str | bytes | None) -> str:
    if isinstance(raw_output, bytes):
        return raw_output.decode("utf-8", errors="replace")
    return raw_output or ""
