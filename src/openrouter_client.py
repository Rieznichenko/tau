from __future__ import annotations

import logging
from typing import Any

import httpx

log = logging.getLogger("swe-eval.openrouter_client")

_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
_DEFAULT_MODEL = "anthropic/claude-sonnet-4"


def complete_text(
    *,
    prompt: str,
    model: str | None,
    timeout: int,
    openrouter_api_key: str,
    system_prompt: str | None = None,
) -> str:
    payload: dict[str, Any] = {
        "model": _resolve_model(model),
        "messages": _build_messages(system_prompt=system_prompt, prompt=prompt),
    }
    headers = {
        "Authorization": f"Bearer {openrouter_api_key}",
        "Content-Type": "application/json",
        "X-Title": "swe-eval",
    }
    log.debug("Calling OpenRouter model=%s timeout=%ss", payload["model"], timeout)
    with httpx.Client(timeout=timeout) as client:
        response = client.post(_OPENROUTER_URL, headers=headers, json=payload)
        response.raise_for_status()
    data = response.json()
    choices = data.get("choices") or []
    if not choices:
        raise RuntimeError("OpenRouter returned no choices")
    message = choices[0].get("message") or {}
    content = message.get("content")
    text = _extract_text(content)
    if not text.strip():
        raise RuntimeError("OpenRouter returned empty content")
    return text


def _resolve_model(model: str | None) -> str:
    if not model:
        return _DEFAULT_MODEL
    if model.startswith("openrouter/"):
        return model.split("/", 1)[1]
    return model


def _build_messages(*, system_prompt: str | None, prompt: str) -> list[dict[str, str]]:
    messages: list[dict[str, str]] = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})
    return messages


def _extract_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "text" and item.get("text"):
                parts.append(str(item["text"]))
        return "".join(parts)
    return ""
