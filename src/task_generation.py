from __future__ import annotations

import json
import logging
import re
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path

from claude_runner import run_claude
from github_miner import CommitCandidate
from openrouter_client import complete_text

log = logging.getLogger("swe-eval.task_generation")


_JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*\n(.*?)```", re.DOTALL)


@dataclass(slots=True)
class GeneratedTask:
    title: str
    description: str
    acceptance_criteria: list[str]
    raw_output: str
    elapsed_seconds: float

    @property
    def prompt_text(self) -> str:
        criteria = "\n".join(f"- {item}" for item in self.acceptance_criteria)
        return (
            f"{self.title}\n\n"
            f"{self.description.strip()}\n\n"
            f"Acceptance criteria:\n{criteria}"
        ).strip()

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "description": self.description,
            "acceptance_criteria": self.acceptance_criteria,
            "raw_output": self.raw_output,
            "elapsed_seconds": self.elapsed_seconds,
            "prompt_text": self.prompt_text,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "GeneratedTask":
        return cls(
            title=str(payload.get("title") or ""),
            description=str(payload.get("description") or ""),
            acceptance_criteria=[
                str(item).strip()
                for item in (payload.get("acceptance_criteria") or [])
                if str(item).strip()
            ],
            raw_output=str(payload.get("raw_output") or ""),
            elapsed_seconds=float(payload.get("elapsed_seconds") or 0.0),
        )


def generate_task_description(
    *,
    candidate: CommitCandidate,
    prompt_dir: Path,
    model: str | None,
    timeout: int,
    openrouter_api_key: str | None = None,
) -> GeneratedTask:
    prompt = _build_generation_prompt(candidate)
    log.debug("Writing task generation prompt to %s", prompt_dir / "task_generation_prompt.txt")
    (prompt_dir / "task_generation_prompt.txt").write_text(prompt + "\n")
    start = time.monotonic()
    log.debug("Invoking task generation Claude runner for %s", candidate.repo_full_name)
    result = _run_claude(
        prompt=prompt,
        workspace=prompt_dir,
        model=model,
        timeout=timeout,
        openrouter_api_key=openrouter_api_key,
    )
    elapsed = time.monotonic() - start
    (prompt_dir / "task_generation_raw.txt").write_text((result or "") + "\n")
    payload = _extract_json_object(result)

    if payload is None:
        log.debug("Task generation returned non-JSON output; using fallback task")
        return _fallback_task(candidate=candidate, raw_output=result, elapsed=elapsed)

    title = str(payload.get("title") or _default_title(candidate))
    description = str(payload.get("description") or "").strip()
    acceptance = payload.get("acceptance_criteria") or []
    if not isinstance(acceptance, list):
        acceptance = []
    acceptance_criteria = [str(item).strip() for item in acceptance if str(item).strip()]
    if not description:
        log.debug("Task generation JSON was missing description; using fallback task")
        return _fallback_task(candidate=candidate, raw_output=result, elapsed=elapsed)
    if not acceptance_criteria:
        acceptance_criteria = ["Match the behavior implied by the mined upstream change."]
    log.debug("Task generation completed in %.2fs", elapsed)
    return GeneratedTask(
        title=title,
        description=description,
        acceptance_criteria=acceptance_criteria,
        raw_output=result,
        elapsed_seconds=elapsed,
    )


def _fallback_task(*, candidate: CommitCandidate, raw_output: str, elapsed: float) -> GeneratedTask:
    changed_files = ", ".join(candidate.changed_files[:8]) or "the affected files"
    clean_output = raw_output.strip()
    description = clean_output or (
        f"Update the repository so it reflects the behavior implied by commit "
        f"`{candidate.short_sha}`. Focus on the changes touching {changed_files} "
        f"and make the working tree behave like the mined upstream revision."
    )
    return GeneratedTask(
        title=_default_title(candidate),
        description=description,
        acceptance_criteria=[
            "Make the code and/or data changes needed to match the intended upstream behavior.",
            f"Limit the implementation to the areas related to {changed_files}.",
        ],
        raw_output=raw_output,
        elapsed_seconds=elapsed,
    )


def _default_title(candidate: CommitCandidate) -> str:
    message = candidate.message.splitlines()[0].strip()
    if message:
        return message[:100]
    return f"Reproduce the behavior from {candidate.short_sha}"


def _build_generation_prompt(candidate: CommitCandidate) -> str:
    files = "\n".join(f"- {name}" for name in candidate.changed_files[:40])
    return textwrap.dedent(
        f"""\
        You are turning a real Git commit into a SWE-style coding task.

        Write a task description for another coding agent that should reproduce
        the behavior of this change without seeing the final answer.

        Repository: {candidate.repo_full_name}
        Commit message: {candidate.message or "(no message)"}
        Changed files:
        {files}

        Diff:
        {candidate.combined_patch}

        Return valid JSON only with this exact shape:
        {{
          "title": "short task title",
          "description": "2-5 paragraph user-facing task description",
          "acceptance_criteria": ["criterion 1", "criterion 2", "criterion 3"]
        }}

        Rules:
        - Describe the intended behavior, bug, or feature in natural language.
        - Do not mention commits, patches, shas, upstream, or diff hunks.
        - Do not reveal the exact implementation strategy unless required by the behavior.
        - Focus on what should be true after the fix.
        """,
    )


def _run_claude(
    *,
    prompt: str,
    workspace: Path,
    model: str | None,
    timeout: int,
    openrouter_api_key: str | None,
) -> str:
    if openrouter_api_key:
        return complete_text(
            prompt=prompt,
            model=model,
            timeout=timeout,
            openrouter_api_key=openrouter_api_key,
        )
    result = run_claude(
        prompt=prompt,
        cwd=workspace,
        model=model,
        timeout=timeout,
        output_format="text",
        openrouter_api_key=openrouter_api_key,
    )
    output = result.combined_output
    if not output.strip():
        raise RuntimeError("Task generation returned empty output from Claude")
    if result.returncode != 0:
        raise RuntimeError(f"Task generation failed with exit code {result.returncode}")
    log.debug("Task generation Claude runner exited with code %s", result.returncode)
    return output


def _extract_json_object(raw_output: str) -> dict | None:
    candidates = [raw_output]
    candidates.extend(match.group(1).strip() for match in _JSON_BLOCK_RE.finditer(raw_output))
    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return None
