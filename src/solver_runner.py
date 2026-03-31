from __future__ import annotations

import json
import logging
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from claude_runner import run_claude
from config import RunConfig
from openrouter_proxy import SolveBudget, SolveUsageSummary
from task_generation import GeneratedTask
from workspace import git_diff

log = logging.getLogger("swe-eval.solver_runner")
COMPLETED_EXIT_REASON = "completed"
TIME_LIMIT_EXIT_REASON = "time_limit_exceeded"
SANDBOX_VIOLATION_EXIT_REASON = "sandbox_violation"
SOLVER_ERROR_EXIT_REASON = "solver_error"


@dataclass(slots=True)
class SolveResult:
    success: bool
    elapsed_seconds: float
    raw_output: str
    model: str | None
    solution_diff: str
    exit_reason: str = COMPLETED_EXIT_REASON
    usage_summary: SolveUsageSummary | None = None
    request_count: int | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    cached_tokens: int | None = None
    cache_write_tokens: int | None = None
    reasoning_tokens: int | None = None
    cost: float | None = None
    tool_calls: int | None = None

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "elapsed_seconds": self.elapsed_seconds,
            "raw_output": self.raw_output,
            "model": self.model,
            "solution_diff": self.solution_diff,
            "exit_reason": self.exit_reason,
            "usage_summary": self.usage_summary.to_dict() if self.usage_summary else None,
            "request_count": self.request_count,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "cached_tokens": self.cached_tokens,
            "cache_write_tokens": self.cache_write_tokens,
            "reasoning_tokens": self.reasoning_tokens,
            "cost": self.cost,
            "tool_calls": self.tool_calls,
        }


def solve_task(
    *,
    repo_dir: Path,
    task: GeneratedTask,
    model: str | None,
    timeout: int,
    config: RunConfig | None = None,
) -> SolveResult:
    prompt = build_solver_prompt(task)
    log.debug("Prepared solver prompt for task %r", task.title)
    result = run_claude(
        prompt=prompt,
        cwd=repo_dir,
        model=model,
        timeout=timeout,
        output_format="text",
        openrouter_api_key=config.openrouter_api_key if config else None,
        solve_budget=SolveBudget.from_config(config),
    )

    raw_output, parsed_total_tokens, tool_calls = _parse_claude_json_output(result.stdout)
    if not raw_output:
        raw_output = result.combined_output
    exit_reason = _resolve_exit_reason(result)
    success = result.returncode == 0 and exit_reason == COMPLETED_EXIT_REASON
    if not raw_output.strip() and success:
        raw_output = "Solver returned empty output from Claude"
        exit_reason = SOLVER_ERROR_EXIT_REASON
        success = False
    solution_diff = git_diff(repo_dir)
    usage_summary = result.usage_summary
    log.debug(
        "Solver exited code=%s elapsed=%.2fs total_tokens=%s tool_calls=%s exit_reason=%s",
        result.returncode,
        result.elapsed_seconds,
        usage_summary.total_tokens if usage_summary else parsed_total_tokens,
        tool_calls,
        exit_reason,
    )

    return SolveResult(
        success=success,
        elapsed_seconds=result.elapsed_seconds,
        raw_output=raw_output,
        model=model,
        solution_diff=solution_diff,
        exit_reason=exit_reason,
        usage_summary=usage_summary,
        request_count=usage_summary.request_count if usage_summary else None,
        prompt_tokens=usage_summary.prompt_tokens if usage_summary else None,
        completion_tokens=usage_summary.completion_tokens if usage_summary else None,
        total_tokens=usage_summary.total_tokens if usage_summary else parsed_total_tokens,
        cached_tokens=usage_summary.cached_tokens if usage_summary else None,
        cache_write_tokens=usage_summary.cache_write_tokens if usage_summary else None,
        reasoning_tokens=usage_summary.reasoning_tokens if usage_summary else None,
        cost=usage_summary.cost if usage_summary else None,
        tool_calls=tool_calls,
    )


def build_solver_prompt(task: GeneratedTask) -> str:
    return textwrap.dedent(
        f"""\
        You are solving a mined software engineering task from inside the
        repository root for this checkout.

        Task:
        {task.prompt_text}

        Requirements:
        - Treat the current workspace as the repository itself and work directly in it.
        - Inspect the repository and implement the requested behavior in this checkout.
        - Stay scoped to this repository; do not rely on sibling workspaces or external patches.
        - Keep changes focused on the task.
        - Do not add explanatory markdown files.
        - When finished, briefly summarize what you changed.
        """,
    )


def _parse_claude_json_output(raw_output: str) -> tuple[str, int | None, int | None]:
    text = raw_output.strip()
    if not text:
        return "", None, None

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return text, None, None

    if not isinstance(payload, dict):
        return text, None, None

    extracted_text = _extract_text(payload).strip() or text
    token_count = _extract_token_count(payload)
    tool_calls = _count_tool_calls(payload)
    return extracted_text, token_count, tool_calls


def _extract_text(payload: Any) -> str:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, list):
        parts = [_extract_text(item).strip() for item in payload]
        return "\n".join(part for part in parts if part)
    if isinstance(payload, dict):
        for key in ("result", "content", "text", "message", "completion"):
            value = payload.get(key)
            if value:
                return _extract_text(value)
        if payload.get("type") == "text":
            return str(payload.get("text") or "")
        if isinstance(payload.get("content"), list):
            return _extract_text(payload["content"])
    return ""


def _extract_token_count(payload: Any) -> int | None:
    usage = _find_usage_dict(payload)
    if not usage:
        return None
    total = usage.get("total_tokens")
    if isinstance(total, int):
        return total
    prompt_tokens = usage.get("input_tokens")
    completion_tokens = usage.get("output_tokens")
    if isinstance(prompt_tokens, int) or isinstance(completion_tokens, int):
        return int(prompt_tokens or 0) + int(completion_tokens or 0)
    return None


def _find_usage_dict(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        usage = payload.get("usage")
        if isinstance(usage, dict):
            return usage
        for value in payload.values():
            nested = _find_usage_dict(value)
            if nested:
                return nested
    elif isinstance(payload, list):
        for item in payload:
            nested = _find_usage_dict(item)
            if nested:
                return nested
    return None


def _count_tool_calls(payload: Any) -> int | None:
    count = _count_tool_calls_inner(payload)
    return count or None


def _count_tool_calls_inner(payload: Any) -> int:
    if isinstance(payload, list):
        return sum(_count_tool_calls_inner(item) for item in payload)
    if not isinstance(payload, dict):
        return 0

    count = 0
    entry_type = payload.get("type")
    if entry_type in {"tool_call", "tool_use"}:
        count += 1
    tool_calls = payload.get("tool_calls")
    if isinstance(tool_calls, list):
        count += len(tool_calls)
    for value in payload.values():
        count += _count_tool_calls_inner(value)
    return count


def _resolve_exit_reason(result) -> str:
    if result.timed_out:
        return TIME_LIMIT_EXIT_REASON
    if result.budget_exceeded_reason:
        return result.budget_exceeded_reason
    if result.returncode == 0:
        return COMPLETED_EXIT_REASON
    return SOLVER_ERROR_EXIT_REASON
