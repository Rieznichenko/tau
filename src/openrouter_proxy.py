from __future__ import annotations

import json
import logging
import os
import secrets
import socketserver
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx

log = logging.getLogger("swe-eval.openrouter_proxy")

_UPSTREAM_BASE_URL = "https://openrouter.ai/api"
_UPSTREAM_TIMEOUT = httpx.Timeout(None)
REQUEST_LIMIT_EXIT_REASON = "request_limit_exceeded"
TOKEN_LIMIT_EXIT_REASON = "token_limit_exceeded"
COST_LIMIT_EXIT_REASON = "cost_limit_exceeded"
PROXY_ERROR_EXIT_REASON = "proxy_error"
_MAX_REQUEST_BODY_BYTES = 2 * 1024 * 1024
_ALLOWED_METHODS = {"POST"}
_ALLOWED_PATHS = {"/v1/chat/completions"}
_ESTIMATED_CHARS_PER_TOKEN = 3
_ESTIMATED_MESSAGE_OVERHEAD_TOKENS = 8
_ESTIMATED_TOOL_OVERHEAD_TOKENS = 24
_HOP_BY_HOP_HEADERS = {
    "connection",
    "content-length",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


class _ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


class _ReusableThreadingUnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    allow_reuse_address = True
    daemon_threads = True


@dataclass(slots=True)
class SolveBudget:
    max_requests: int | None = None
    max_total_tokens: int | None = None
    max_prompt_tokens: int | None = None
    max_completion_tokens: int | None = None
    max_cost: float | None = None
    max_tokens_per_request: int | None = None

    def enabled(self) -> bool:
        return any(
            value is not None
            for value in (
                self.max_requests,
                self.max_total_tokens,
                self.max_prompt_tokens,
                self.max_completion_tokens,
                self.max_cost,
                self.max_tokens_per_request,
            )
        )

    @classmethod
    def from_config(cls, config: Any | None) -> SolveBudget | None:
        if config is None:
            return None
        budget = cls(
            max_requests=getattr(config, "solver_max_requests", None),
            max_total_tokens=getattr(config, "solver_max_total_tokens", None),
            max_prompt_tokens=getattr(config, "solver_max_prompt_tokens", None),
            max_completion_tokens=getattr(config, "solver_max_completion_tokens", None),
            max_cost=getattr(config, "solver_max_cost", None),
            max_tokens_per_request=getattr(config, "solver_max_tokens_per_request", None),
        )
        return budget if budget.enabled() else None


@dataclass(slots=True)
class ProxyRequestRecord:
    method: str
    path: str
    status_code: int | None
    latency_ms: int
    request_model: str | None = None
    response_model: str | None = None
    generation_id: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    cached_tokens: int | None = None
    cache_write_tokens: int | None = None
    reasoning_tokens: int | None = None
    cost: float | None = None
    rejected: bool = False
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "path": self.path,
            "status_code": self.status_code,
            "latency_ms": self.latency_ms,
            "request_model": self.request_model,
            "response_model": self.response_model,
            "generation_id": self.generation_id,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "cached_tokens": self.cached_tokens,
            "cache_write_tokens": self.cache_write_tokens,
            "reasoning_tokens": self.reasoning_tokens,
            "cost": self.cost,
            "rejected": self.rejected,
            "error": self.error,
        }


@dataclass(slots=True)
class SolveUsageSummary:
    request_count: int = 0
    rejected_request_count: int = 0
    success_count: int = 0
    error_count: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cached_tokens: int = 0
    cache_write_tokens: int = 0
    reasoning_tokens: int = 0
    cost: float = 0.0
    budget_exceeded_reason: str | None = None
    requests: list[ProxyRequestRecord] = field(default_factory=list)

    def snapshot(self) -> SolveUsageSummary:
        return SolveUsageSummary(
            request_count=self.request_count,
            rejected_request_count=self.rejected_request_count,
            success_count=self.success_count,
            error_count=self.error_count,
            prompt_tokens=self.prompt_tokens,
            completion_tokens=self.completion_tokens,
            total_tokens=self.total_tokens,
            cached_tokens=self.cached_tokens,
            cache_write_tokens=self.cache_write_tokens,
            reasoning_tokens=self.reasoning_tokens,
            cost=self.cost,
            budget_exceeded_reason=self.budget_exceeded_reason,
            requests=[
                ProxyRequestRecord(**request.to_dict())
                for request in self.requests
            ],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_count": self.request_count,
            "rejected_request_count": self.rejected_request_count,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "cached_tokens": self.cached_tokens,
            "cache_write_tokens": self.cache_write_tokens,
            "reasoning_tokens": self.reasoning_tokens,
            "cost": self.cost,
            "budget_exceeded_reason": self.budget_exceeded_reason,
            "requests": [request.to_dict() for request in self.requests],
        }


@dataclass(slots=True)
class OpenRouterProxy:
    openrouter_api_key: str
    solve_budget: SolveBudget | None = None
    bind_host: str | None = "127.0.0.1"
    bind_port: int = 0
    unix_socket_path: str | None = None
    enforced_model: str | None = None
    require_auth: bool = True
    auth_token: str = field(default_factory=lambda: secrets.token_urlsafe(24))
    _server: _ReusableThreadingHTTPServer | None = field(default=None, init=False, repr=False)
    _unix_server: _ReusableThreadingUnixServer | None = field(default=None, init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _unix_thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _usage: SolveUsageSummary = field(default_factory=SolveUsageSummary, init=False, repr=False)

    def start(self) -> None:
        if self._server is not None or self._unix_server is not None:
            return

        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:  # noqa: N802
                self._handle()

            def do_POST(self) -> None:  # noqa: N802
                self._handle()

            def log_message(self, format: str, *args: object) -> None:
                log.debug("proxy %s - %s", self.address_string(), format % args)

            def _handle(self) -> None:
                try:
                    proxy._handle_request(self)
                except Exception:  # noqa: BLE001
                    log.exception("OpenRouter proxy request failed")
                    self.send_error(502, "Proxy request failed")

        if self.bind_host is not None:
            self._server = _ReusableThreadingHTTPServer((self.bind_host, self.bind_port), Handler)
            self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
            self._thread.start()
            log.debug("OpenRouter proxy listening on %s:%s", self.host, self.port)

        if self.unix_socket_path:
            socket_dir = os.path.dirname(self.unix_socket_path)
            if socket_dir:
                os.makedirs(socket_dir, exist_ok=True)
            if os.path.exists(self.unix_socket_path):
                os.unlink(self.unix_socket_path)
            self._unix_server = _ReusableThreadingUnixServer(self.unix_socket_path, Handler)
            self._unix_thread = threading.Thread(target=self._unix_server.serve_forever, daemon=True)
            self._unix_thread.start()
            log.debug("OpenRouter proxy listening on unix socket %s", self.unix_socket_path)

    def stop(self) -> None:
        if self._server is None and self._unix_server is None:
            return

        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._unix_server is not None:
            self._unix_server.shutdown()
            self._unix_server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._unix_thread is not None:
            self._unix_thread.join(timeout=5)
        self._server = None
        self._unix_server = None
        self._thread = None
        self._unix_thread = None
        if self.unix_socket_path and os.path.exists(self.unix_socket_path):
            os.unlink(self.unix_socket_path)

    def __enter__(self) -> OpenRouterProxy:
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    @property
    def host(self) -> str:
        if self._server is None:
            raise RuntimeError("Proxy server is not running")
        return self._server.server_address[0]

    @property
    def port(self) -> int:
        if self._server is None:
            raise RuntimeError("Proxy server is not running")
        return int(self._server.server_address[1])

    def container_base_url(self, host_name: str) -> str:
        return f"http://{host_name}:{self.port}/v1"

    def usage_snapshot(self) -> SolveUsageSummary:
        with self._lock:
            return self._usage.snapshot()

    @property
    def budget_exceeded_reason(self) -> str | None:
        with self._lock:
            return self._usage.budget_exceeded_reason

    def _handle_request(self, handler: BaseHTTPRequestHandler) -> None:
        if handler.command not in _ALLOWED_METHODS:
            self._reject_request(
                handler,
                reason=PROXY_ERROR_EXIT_REASON,
                status=405,
                error_type="proxy_policy_violation",
                message="Method not allowed",
                method=handler.command,
                path=handler.path,
                request_model=None,
            )
            return
        if handler.path not in _ALLOWED_PATHS:
            self._reject_request(
                handler,
                reason=PROXY_ERROR_EXIT_REASON,
                status=403,
                error_type="proxy_policy_violation",
                message="Endpoint not allowed",
                method=handler.command,
                path=handler.path,
                request_model=None,
            )
            return

        if self.require_auth:
            expected_auth = f"Bearer {self.auth_token}"
            auth_header = handler.headers.get("Authorization")
            api_key_header = handler.headers.get("x-api-key")
            if auth_header != expected_auth and api_key_header != self.auth_token:
                self._reject_request(
                    handler,
                    reason=PROXY_ERROR_EXIT_REASON,
                    status=401,
                    error_type="proxy_policy_violation",
                    message="Unauthorized",
                    method=handler.command,
                    path=handler.path,
                    request_model=None,
                )
                return

        try:
            content_length = int(handler.headers.get("Content-Length", "0") or "0")
        except ValueError:
            self._reject_request(
                handler,
                reason=PROXY_ERROR_EXIT_REASON,
                status=400,
                error_type="proxy_policy_violation",
                message="Invalid Content-Length header",
                method=handler.command,
                path=handler.path,
                request_model=None,
            )
            return
        if content_length < 0:
            self._reject_request(
                handler,
                reason=PROXY_ERROR_EXIT_REASON,
                status=400,
                error_type="proxy_policy_violation",
                message="Invalid Content-Length header",
                method=handler.command,
                path=handler.path,
                request_model=None,
            )
            return
        if content_length > _MAX_REQUEST_BODY_BYTES:
            self._reject_request(
                handler,
                reason=PROXY_ERROR_EXIT_REASON,
                status=413,
                error_type="proxy_policy_violation",
                message="Request body too large",
                method=handler.command,
                path=handler.path,
                request_model=None,
            )
            return
        body = handler.rfile.read(content_length) if content_length > 0 else None
        request_payload = _loads_json_bytes(body)
        if content_length > 0 and not isinstance(request_payload, dict):
            self._reject_request(
                handler,
                reason=PROXY_ERROR_EXIT_REASON,
                status=400,
                error_type="proxy_policy_violation",
                message="Request body must be a JSON object",
                method=handler.command,
                path=handler.path,
                request_model=None,
            )
            return
        request_model = _extract_request_model(request_payload)
        if (not request_model and not self.enforced_model) or not _request_payload_has_messages(request_payload):
            self._reject_request(
                handler,
                reason=PROXY_ERROR_EXIT_REASON,
                status=400,
                error_type="proxy_policy_violation",
                message="Request body must include model and messages",
                method=handler.command,
                path=handler.path,
                request_model=request_model,
            )
            return
        body, rejection_reason = self._prepare_request_body(body=body, request_payload=request_payload)
        if rejection_reason:
            self._reject_request(
                handler,
                reason=rejection_reason,
                status=429,
                error_type="budget_exceeded",
                message="Solve budget exceeded",
                method=handler.command,
                path=handler.path,
                request_model=request_model,
            )
            return
        prepared_payload = _loads_json_bytes(body)
        if isinstance(prepared_payload, dict):
            request_model = _extract_request_model(prepared_payload) or request_model
        upstream_url = f"{_UPSTREAM_BASE_URL}{handler.path}"
        upstream_headers = self._build_upstream_headers(handler)
        start = time.monotonic()

        try:
            with httpx.Client(timeout=_UPSTREAM_TIMEOUT) as client:
                response = client.request(
                    handler.command,
                    upstream_url,
                    headers=upstream_headers,
                    content=body,
                )
        except httpx.HTTPError as exc:
            latency_ms = int((time.monotonic() - start) * 1000)
            self._record_request(
                ProxyRequestRecord(
                    method=handler.command,
                    path=handler.path,
                    status_code=None,
                    latency_ms=latency_ms,
                    request_model=request_model,
                    error=str(exc),
                ),
            )
            raise

        response_body = response.content
        latency_ms = int((time.monotonic() - start) * 1000)
        response_payload = _loads_json_bytes(response_body)
        request_record = ProxyRequestRecord(
            method=handler.command,
            path=handler.path,
            status_code=response.status_code,
            latency_ms=latency_ms,
            request_model=request_model,
            response_model=_extract_response_model(response_payload),
            generation_id=_extract_generation_id(response_payload),
            prompt_tokens=_extract_prompt_tokens(response_payload),
            completion_tokens=_extract_completion_tokens(response_payload),
            total_tokens=_extract_total_tokens(response_payload),
            cached_tokens=_extract_cached_tokens(response_payload),
            cache_write_tokens=_extract_cache_write_tokens(response_payload),
            reasoning_tokens=_extract_reasoning_tokens(response_payload),
            cost=_extract_cost(response_payload),
        )
        self._record_request(request_record)

        handler.send_response(response.status_code)
        for key, value in response.headers.items():
            if key.lower() in _HOP_BY_HOP_HEADERS or key.lower() == "content-length":
                continue
            handler.send_header(key, value)
        handler.send_header("Content-Length", str(len(response_body)))
        handler.send_header("Connection", "close")
        handler.end_headers()
        handler.wfile.write(response_body)
        handler.wfile.flush()
        handler.close_connection = True

    def _build_upstream_headers(self, handler: BaseHTTPRequestHandler) -> dict[str, str]:
        headers: dict[str, str] = {
            "Authorization": f"Bearer {self.openrouter_api_key}",
            "X-Title": "swe-eval",
        }
        for key, value in handler.headers.items():
            lowered = key.lower()
            if lowered in _HOP_BY_HOP_HEADERS or lowered in {"authorization", "x-api-key"}:
                continue
            headers[key] = value
        return headers

    def _prepare_request_body(
        self,
        *,
        body: bytes | None,
        request_payload: Any,
    ) -> tuple[bytes | None, str | None]:
        if isinstance(request_payload, dict) and self.enforced_model:
            request_payload["model"] = self.enforced_model
            body = json.dumps(request_payload).encode("utf-8")
        if not self.solve_budget or not self.solve_budget.enabled():
            with self._lock:
                if self._usage.budget_exceeded_reason:
                    return body, self._usage.budget_exceeded_reason
                self._check_request_limit_locked()
                if self._usage.budget_exceeded_reason:
                    return body, self._usage.budget_exceeded_reason
                self._usage.request_count += 1
            return body, None

        if not isinstance(request_payload, dict):
            with self._lock:
                self._check_pre_request_budget_locked()
                if self._usage.budget_exceeded_reason:
                    return body, self._usage.budget_exceeded_reason
                self._check_request_limit_locked()
                if self._usage.budget_exceeded_reason:
                    return body, self._usage.budget_exceeded_reason
                self._usage.request_count += 1
            return body, None

        estimated_prompt_tokens = _estimate_prompt_tokens(request_payload)
        with self._lock:
            self._check_pre_request_budget_locked()
            if self._usage.budget_exceeded_reason:
                return body, self._usage.budget_exceeded_reason
            self._check_request_limit_locked()
            if self._usage.budget_exceeded_reason:
                return body, self._usage.budget_exceeded_reason
            self._check_estimated_request_budget_locked(
                estimated_prompt_tokens=estimated_prompt_tokens,
                request_payload=request_payload,
            )
            if self._usage.budget_exceeded_reason:
                return body, self._usage.budget_exceeded_reason
            self._clamp_request_tokens_locked(
                request_payload=request_payload,
                estimated_prompt_tokens=estimated_prompt_tokens,
            )
            if self._usage.budget_exceeded_reason:
                return body, self._usage.budget_exceeded_reason
            self._usage.request_count += 1

        return json.dumps(request_payload).encode("utf-8"), None

    def _check_pre_request_budget_locked(self) -> None:
        if self._usage.budget_exceeded_reason:
            return
        if not self.solve_budget:
            return
        if self.solve_budget.max_cost is not None and self._usage.cost >= self.solve_budget.max_cost:
            self._usage.budget_exceeded_reason = COST_LIMIT_EXIT_REASON
            return
        if self.solve_budget.max_total_tokens is not None and self._usage.total_tokens >= self.solve_budget.max_total_tokens:
            self._usage.budget_exceeded_reason = TOKEN_LIMIT_EXIT_REASON
            return
        if self.solve_budget.max_prompt_tokens is not None and self._usage.prompt_tokens >= self.solve_budget.max_prompt_tokens:
            self._usage.budget_exceeded_reason = TOKEN_LIMIT_EXIT_REASON
            return
        if (
            self.solve_budget.max_completion_tokens is not None
            and self._usage.completion_tokens >= self.solve_budget.max_completion_tokens
        ):
            self._usage.budget_exceeded_reason = TOKEN_LIMIT_EXIT_REASON

    def _check_request_limit_locked(self) -> None:
        if self._usage.budget_exceeded_reason or not self.solve_budget:
            return
        if self.solve_budget.max_requests is not None and self._usage.request_count >= self.solve_budget.max_requests:
            self._usage.budget_exceeded_reason = REQUEST_LIMIT_EXIT_REASON

    def _check_estimated_request_budget_locked(
        self,
        *,
        estimated_prompt_tokens: int,
        request_payload: dict[str, Any],
    ) -> None:
        if not self.solve_budget or self._usage.budget_exceeded_reason:
            return
        remaining_prompt_tokens = None
        if self.solve_budget.max_prompt_tokens is not None:
            remaining_prompt_tokens = max(0, self.solve_budget.max_prompt_tokens - self._usage.prompt_tokens)
            if estimated_prompt_tokens > remaining_prompt_tokens:
                self._usage.budget_exceeded_reason = TOKEN_LIMIT_EXIT_REASON
                return

        remaining_total_tokens = None
        if self.solve_budget.max_total_tokens is not None:
            remaining_total_tokens = max(0, self.solve_budget.max_total_tokens - self._usage.total_tokens)
            if estimated_prompt_tokens >= remaining_total_tokens:
                self._usage.budget_exceeded_reason = TOKEN_LIMIT_EXIT_REASON
                return

        requested_max_output_tokens = _extract_requested_max_output_tokens(request_payload)
        if requested_max_output_tokens is None or remaining_total_tokens is None:
            return
        if estimated_prompt_tokens + requested_max_output_tokens > remaining_total_tokens:
            log.debug(
                "Estimated request would exceed total token budget prompt_estimate=%s requested_max_output=%s remaining_total=%s",
                estimated_prompt_tokens,
                requested_max_output_tokens,
                remaining_total_tokens,
            )

    def _clamp_request_tokens_locked(
        self,
        *,
        request_payload: dict[str, Any],
        estimated_prompt_tokens: int,
    ) -> None:
        if not self.solve_budget:
            return
        limits: list[int] = []
        if self.solve_budget.max_tokens_per_request is not None:
            limits.append(self.solve_budget.max_tokens_per_request)
        if self.solve_budget.max_completion_tokens is not None:
            limits.append(max(0, self.solve_budget.max_completion_tokens - self._usage.completion_tokens))
        if self.solve_budget.max_total_tokens is not None:
            remaining_total_tokens = max(0, self.solve_budget.max_total_tokens - self._usage.total_tokens)
            limits.append(max(0, remaining_total_tokens - estimated_prompt_tokens))
        average_cost_per_token = self._average_cost_per_token_locked()
        if self.solve_budget.max_cost is not None and average_cost_per_token is not None and average_cost_per_token > 0:
            remaining_cost = max(0.0, self.solve_budget.max_cost - self._usage.cost)
            estimated_prompt_cost = estimated_prompt_tokens * average_cost_per_token
            if estimated_prompt_cost >= remaining_cost:
                self._usage.budget_exceeded_reason = COST_LIMIT_EXIT_REASON
                return
            estimated_affordable_output_tokens = int((remaining_cost - estimated_prompt_cost) / average_cost_per_token)
            limits.append(max(0, estimated_affordable_output_tokens))
        if not limits:
            return
        allowed_max_tokens = min(limits)
        if allowed_max_tokens <= 0:
            self._usage.budget_exceeded_reason = TOKEN_LIMIT_EXIT_REASON
            return
        _set_requested_max_output_tokens(request_payload, allowed_max_tokens)

    def _average_cost_per_token_locked(self) -> float | None:
        if self._usage.total_tokens <= 0 or self._usage.cost <= 0:
            return None
        return self._usage.cost / self._usage.total_tokens

    def _reject_request(
        self,
        handler: BaseHTTPRequestHandler,
        *,
        reason: str,
        status: int,
        error_type: str,
        message: str,
        method: str,
        path: str,
        request_model: str | None,
    ) -> None:
        with self._lock:
            self._usage.budget_exceeded_reason = reason
            self._usage.rejected_request_count += 1
            self._usage.requests.append(
                ProxyRequestRecord(
                    method=method,
                    path=path,
                    status_code=429,
                    latency_ms=0,
                    request_model=request_model,
                    rejected=True,
                    error=reason,
                ),
            )
        self._send_json(
            handler,
            status,
            {
                "error": {
                    "message": message,
                    "type": error_type,
                    "code": reason,
                },
            },
        )

    def _record_request(self, request: ProxyRequestRecord) -> None:
        with self._lock:
            self._usage.requests.append(request)
            if request.status_code is not None and request.status_code < 400 and request.error is None:
                self._usage.success_count += 1
            else:
                self._usage.error_count += 1
            self._usage.prompt_tokens += int(request.prompt_tokens or 0)
            self._usage.completion_tokens += int(request.completion_tokens or 0)
            self._usage.total_tokens += int(request.total_tokens or 0)
            self._usage.cached_tokens += int(request.cached_tokens or 0)
            self._usage.cache_write_tokens += int(request.cache_write_tokens or 0)
            self._usage.reasoning_tokens += int(request.reasoning_tokens or 0)
            self._usage.cost += float(request.cost or 0.0)
            self._check_pre_request_budget_locked()

    @staticmethod
    def _send_json(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(body)))
        handler.send_header("Connection", "close")
        handler.end_headers()
        handler.wfile.write(body)
        handler.wfile.flush()
        handler.close_connection = True


def _loads_json_bytes(raw_body: bytes | None) -> Any:
    if not raw_body:
        return None
    try:
        return json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _extract_request_model(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    model = payload.get("model")
    return str(model) if isinstance(model, str) else None


def _extract_response_model(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    model = payload.get("model")
    return str(model) if isinstance(model, str) else None


def _extract_generation_id(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    generation_id = payload.get("id")
    return str(generation_id) if isinstance(generation_id, str) else None


def _extract_usage(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    usage = payload.get("usage")
    return usage if isinstance(usage, dict) else None


def _extract_prompt_tokens(payload: Any) -> int | None:
    usage = _extract_usage(payload)
    value = usage.get("prompt_tokens") if usage else None
    return value if isinstance(value, int) else None


def _extract_completion_tokens(payload: Any) -> int | None:
    usage = _extract_usage(payload)
    value = usage.get("completion_tokens") if usage else None
    return value if isinstance(value, int) else None


def _extract_total_tokens(payload: Any) -> int | None:
    usage = _extract_usage(payload)
    if not usage:
        return None
    total_tokens = usage.get("total_tokens")
    if isinstance(total_tokens, int):
        return total_tokens
    prompt_tokens = usage.get("prompt_tokens")
    completion_tokens = usage.get("completion_tokens")
    if isinstance(prompt_tokens, int) or isinstance(completion_tokens, int):
        return int(prompt_tokens or 0) + int(completion_tokens or 0)
    return None


def _extract_cached_tokens(payload: Any) -> int | None:
    usage = _extract_usage(payload)
    if not usage:
        return None
    details = usage.get("prompt_tokens_details")
    if not isinstance(details, dict):
        return None
    value = details.get("cached_tokens")
    return value if isinstance(value, int) else None


def _extract_cache_write_tokens(payload: Any) -> int | None:
    usage = _extract_usage(payload)
    if not usage:
        return None
    details = usage.get("prompt_tokens_details")
    if not isinstance(details, dict):
        return None
    value = details.get("cache_write_tokens")
    return value if isinstance(value, int) else None


def _extract_reasoning_tokens(payload: Any) -> int | None:
    usage = _extract_usage(payload)
    if not usage:
        return None
    details = usage.get("completion_tokens_details")
    if not isinstance(details, dict):
        return None
    value = details.get("reasoning_tokens")
    return value if isinstance(value, int) else None


def _extract_cost(payload: Any) -> float | None:
    usage = _extract_usage(payload)
    if not usage:
        return None
    value = usage.get("cost")
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _request_payload_has_messages(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    messages = payload.get("messages")
    return isinstance(messages, list) and len(messages) > 0


def _estimate_prompt_tokens(payload: dict[str, Any]) -> int:
    message_count = 0
    total_chars = 0
    messages = payload.get("messages")
    if isinstance(messages, list):
        message_count = len(messages)
        total_chars += sum(_estimate_content_chars(message) for message in messages)
    tools = payload.get("tools")
    if isinstance(tools, list):
        total_chars += sum(len(json.dumps(tool, sort_keys=True)) for tool in tools)
    response_format = payload.get("response_format")
    if response_format is not None:
        total_chars += len(json.dumps(response_format, sort_keys=True))
    prompt_tokens = (total_chars + (_ESTIMATED_CHARS_PER_TOKEN - 1)) // _ESTIMATED_CHARS_PER_TOKEN
    prompt_tokens += message_count * _ESTIMATED_MESSAGE_OVERHEAD_TOKENS
    if isinstance(tools, list):
        prompt_tokens += len(tools) * _ESTIMATED_TOOL_OVERHEAD_TOKENS
    return max(prompt_tokens, 1)


def _estimate_content_chars(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, str):
        return len(value)
    if isinstance(value, (int, float, bool)):
        return len(str(value))
    if isinstance(value, dict):
        total = 0
        for key, item in value.items():
            total += len(str(key))
            total += _estimate_content_chars(item)
        return total
    if isinstance(value, list):
        return sum(_estimate_content_chars(item) for item in value)
    return len(str(value))


def _extract_requested_max_output_tokens(payload: dict[str, Any]) -> int | None:
    for key in ("max_tokens", "max_completion_tokens"):
        value = payload.get(key)
        if isinstance(value, int):
            return max(value, 0)
    return None


def _set_requested_max_output_tokens(payload: dict[str, Any], value: int) -> None:
    clamped_value = max(value, 0)
    existing_max_tokens = payload.get("max_tokens")
    if isinstance(existing_max_tokens, int):
        payload["max_tokens"] = min(existing_max_tokens, clamped_value)
    else:
        payload["max_tokens"] = clamped_value

    existing_max_completion_tokens = payload.get("max_completion_tokens")
    if isinstance(existing_max_completion_tokens, int):
        payload["max_completion_tokens"] = min(existing_max_completion_tokens, clamped_value)
