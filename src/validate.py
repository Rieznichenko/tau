from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import bittensor as bt
import httpx

from config import RunConfig, SolverAgentSource
from pipeline import _setup_logging, compare_task_run, generate_task_run, solve_task_run
from r2 import (
    duel_to_summary,
    publish_dashboard_data,
    publish_duel_data,
    publish_duel_index,
    publish_round_data,
)
from workspace import write_json

log = logging.getLogger("swe-eval.validate")
_DEFAULT_GITHUB_AGENT_SUBDIR = "agent"
_GITHUB_COMMIT_RE = re.compile(
    r"^(?P<repo>[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)@(?P<sha>[0-9a-fA-F]{7,64})$"
)
_CURSOR_MODEL_FOR_SONNET4 = "claude-4-sonnet"
_AGENT_TIMEOUT_FLOOR = 300
_MIN_PATCH_LINES = 100


# ---------------------------------------------------------------------------
# Data structures (unchanged for dashboard compatibility)
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ValidatorSubmission:
    hotkey: str
    uid: int
    repo_full_name: str
    repo_url: str
    commit_sha: str
    commitment: str
    commitment_block: int
    local_path: str | None = None

    @property
    def agent_ref(self) -> str:
        return f"{self.repo_full_name}@{self.commit_sha}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ValidatorSubmission:
        return cls(
            hotkey=str(payload["hotkey"]), uid=int(payload["uid"]),
            repo_full_name=str(payload["repo_full_name"]),
            repo_url=str(payload["repo_url"]),
            commit_sha=str(payload["commit_sha"]),
            commitment=str(payload["commitment"]),
            commitment_block=int(payload["commitment_block"]),
            local_path=str(payload["local_path"]) if payload.get("local_path") is not None else None,
        )


@dataclass(slots=True)
class ValidationRoundResult:
    task_name: str
    winner: str
    king_lines: int
    challenger_lines: int
    king_similarity_ratio: float
    challenger_similarity_ratio: float
    king_challenger_similarity: float
    task_root: str
    king_compare_root: str
    challenger_compare_root: str
    cursor_lines: int = 0
    error: str | None = None

    @property
    def scored(self) -> bool:
        return self.error is None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DuelResult:
    duel_id: int
    started_at: str
    finished_at: str
    king_before: ValidatorSubmission
    challenger: ValidatorSubmission
    rounds: list[ValidationRoundResult]
    wins: int
    losses: int
    ties: int
    king_after: ValidatorSubmission
    king_replaced: bool
    disqualification_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "duel_id": self.duel_id, "started_at": self.started_at,
            "finished_at": self.finished_at,
            "king_before": self.king_before.to_dict(),
            "challenger": self.challenger.to_dict(),
            "rounds": [r.to_dict() for r in self.rounds],
            "wins": self.wins, "losses": self.losses, "ties": self.ties,
            "king_after": self.king_after.to_dict(),
            "king_replaced": self.king_replaced,
            "disqualification_reason": self.disqualification_reason,
        }


@dataclass(slots=True)
class ValidatorState:
    current_king: ValidatorSubmission | None = None
    queue: list[ValidatorSubmission] = field(default_factory=list)
    seen_hotkeys: list[str] = field(default_factory=list)
    retired_hotkeys: list[str] = field(default_factory=list)
    disqualified_hotkeys: list[str] = field(default_factory=list)
    locked_commitments: dict[str, str] = field(default_factory=dict)
    last_weight_block: int | None = None
    next_task_index: int = 1
    next_duel_index: int = 1
    king_since: str | None = None
    king_duels_defended: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "current_king": self.current_king.to_dict() if self.current_king else None,
            "queue": [s.to_dict() for s in self.queue],
            "seen_hotkeys": self.seen_hotkeys,
            "retired_hotkeys": self.retired_hotkeys,
            "disqualified_hotkeys": self.disqualified_hotkeys,
            "locked_commitments": self.locked_commitments,
            "last_weight_block": self.last_weight_block,
            "next_task_index": self.next_task_index,
            "next_duel_index": self.next_duel_index,
            "king_since": self.king_since,
            "king_duels_defended": self.king_duels_defended,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ValidatorState:
        ck = payload.get("current_king")
        raw_locked = payload.get("locked_commitments", {})
        return cls(
            current_king=ValidatorSubmission.from_dict(ck) if isinstance(ck, dict) else None,
            queue=[ValidatorSubmission.from_dict(i) for i in payload.get("queue", []) if isinstance(i, dict)],
            seen_hotkeys=[str(i) for i in payload.get("seen_hotkeys", [])],
            retired_hotkeys=[str(i) for i in payload.get("retired_hotkeys", [])],
            disqualified_hotkeys=[str(i) for i in payload.get("disqualified_hotkeys", [])],
            locked_commitments={str(k): str(v) for k, v in raw_locked.items()} if isinstance(raw_locked, dict) else {},
            last_weight_block=int(payload["last_weight_block"]) if payload.get("last_weight_block") is not None else None,
            next_task_index=int(payload.get("next_task_index", 1)),
            next_duel_index=int(payload.get("next_duel_index", 1)),
            king_since=payload.get("king_since"),
            king_duels_defended=int(payload.get("king_duels_defended", 0)),
        )


@dataclass(slots=True)
class ValidatePaths:
    root: Path
    state_path: Path
    duels_dir: Path
    pool_dir: Path


@dataclass(slots=True)
class ValidateStageResult:
    validate_root: str
    king_uid: int
    king_hotkey: str
    king_repo: str
    duel_count: int


@dataclass(slots=True)
class PoolTask:
    task_name: str
    task_root: str
    creation_block: int
    cursor_elapsed: float
    king_lines: int
    king_similarity: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> PoolTask:
        return cls(
            task_name=str(d["task_name"]), task_root=str(d["task_root"]),
            creation_block=int(d["creation_block"]),
            cursor_elapsed=float(d["cursor_elapsed"]),
            king_lines=int(d["king_lines"]),
            king_similarity=float(d["king_similarity"]),
        )


# ---------------------------------------------------------------------------
# Mock classes for offline testing
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class _MockNeuron:
    uid: int

class _MockCommitments:
    def get_all_revealed_commitments(self, netuid: int) -> dict:
        _ = netuid
        return {}

    def get_all_commitments(self, netuid: int) -> dict:
        _ = netuid
        return {}


class _MockSubnets:
    def __init__(self, h2u: dict[str, int]) -> None:
        self._h2u = h2u

    def get_uid_for_hotkey_on_subnet(self, hk: str, netuid: int) -> int | None:
        _ = netuid
        return self._h2u.get(hk)


class _MockNeurons:
    def __init__(self, uids: list[int]) -> None:
        self._uids = uids

    def neurons_lite(self, netuid: int) -> list[_MockNeuron]:
        _ = netuid
        return [_MockNeuron(uid=u) for u in self._uids]


class _MockExtrinsics:
    def set_weights(self, **kw: Any) -> dict:
        return {"mocked": True}

class _MockSubtensorApi:
    def __init__(self) -> None:
        self._block = 1000
        h2u = {"mock-king-hotkey": 1, "mock-challenger-hotkey": 2}
        self.commitments = _MockCommitments()
        self.subnets = _MockSubnets(h2u)
        self.neurons = _MockNeurons(list(h2u.values()))
        self.extrinsics = _MockExtrinsics()
    @property
    def block(self) -> int:
        self._block += 1
        return self._block

    def __enter__(self) -> _MockSubtensorApi:
        return self

    def __exit__(self, *a) -> None:
        pass


# ---------------------------------------------------------------------------
# Task pool
# ---------------------------------------------------------------------------

class TaskPool:
    """Thread-safe pool of pre-solved tasks (generate + cursor + king)."""

    def __init__(self, pool_dir: Path) -> None:
        self._pool_dir = pool_dir
        self._pool_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def size(self) -> int:
        with self._lock:
            return len(list(self._pool_dir.glob("*.json")))

    def add(self, task: PoolTask) -> None:
        path = self._pool_dir / f"{task.task_name}.json"
        write_json(path, task.to_dict())

    def pop(self, min_block: int) -> PoolTask | None:
        with self._lock:
            candidates = []
            for p in sorted(self._pool_dir.glob("*.json")):
                try:
                    d = json.loads(p.read_text())
                    if int(d.get("creation_block", 0)) > min_block:
                        candidates.append((p, d))
                except Exception:
                    p.unlink(missing_ok=True)
            if not candidates:
                return None
            path, data = candidates[0]
            path.unlink(missing_ok=True)
            return PoolTask.from_dict(data)

    def flush(self) -> int:
        with self._lock:
            count = 0
            for p in self._pool_dir.glob("*.json"):
                p.unlink(missing_ok=True)
                count += 1
            return count


# ---------------------------------------------------------------------------
# Pool filler (background thread)
# ---------------------------------------------------------------------------

def _pool_filler_loop(
    config: RunConfig,
    state: ValidatorState,
    pool: TaskPool,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        try:
            if state.current_king is None:
                stop_event.wait(5)
                continue

            if pool.size() >= config.validate_task_pool_target:
                stop_event.wait(2)
                continue

            task_name = _allocate_task_name(state)
            log.info("Pool filler: generating task %s", task_name)

            generate_result = generate_task_run(task_name=task_name, config=config)
            task_root = generate_result.task_root

            ref_patch_path = Path(task_root) / "task" / "reference.patch"
            if _count_patch_lines(ref_patch_path) < _MIN_PATCH_LINES:
                log.info("Pool filler: skipping %s (patch too small)", task_name)
                continue

            cursor_start = time.monotonic()
            solve_task_run(task_name=task_name, solution_name="cursor", config=_build_cursor_config(config))
            cursor_elapsed = time.monotonic() - cursor_start

            king = state.current_king
            if king is None:
                continue

            agent_timeout = max(int(cursor_elapsed * 2) + 1, _AGENT_TIMEOUT_FLOOR)
            king_cfg = replace(_build_agent_config(config, king), agent_timeout=agent_timeout)
            solve_task_run(task_name=task_name, solution_name="king", config=king_cfg)

            king_compare = compare_task_run(task_name=task_name, solution_names=["cursor", "king"], config=config)

            # Get current block for stamping
            try:
                with _open_subtensor(config) as sub:
                    creation_block = sub.block
            except Exception:
                creation_block = 0

            pool.add(PoolTask(
                task_name=task_name,
                task_root=task_root,
                creation_block=creation_block,
                cursor_elapsed=cursor_elapsed,
                king_lines=king_compare.matched_changed_lines,
                king_similarity=king_compare.similarity_ratio,
            ))
            log.info("Pool filler: added %s (pool size: %d)", task_name, pool.size())

        except Exception:
            log.exception("Pool filler: error generating task (retrying)")
            stop_event.wait(5)


# ---------------------------------------------------------------------------
# Duel runner (runs independently per challenger)
# ---------------------------------------------------------------------------

def _run_duel(
    *,
    config: RunConfig,
    state: ValidatorState,
    king: ValidatorSubmission,
    challenger: ValidatorSubmission,
    duel_id: int,
    pool: TaskPool,
    cancel_event: threading.Event,
    on_round_complete: Any = None,
) -> DuelResult:
    threshold = config.validate_duel_rounds // 2 + config.validate_win_margin + 1
    started_at = _timestamp()
    rounds: list[ValidationRoundResult] = []
    wins = losses = ties = scored = 0

    log.info("Duel %d: king uid=%s vs challenger uid=%s (%s), need %d/%d wins",
             duel_id, king.uid, challenger.uid, challenger.repo_full_name,
             threshold, config.validate_duel_rounds)

    while scored < config.validate_duel_rounds and not cancel_event.is_set():
        # Pop a task from the pool (must be created after challenger committed)
        task = pool.pop(min_block=challenger.commitment_block)
        if task is None:
            cancel_event.wait(3)
            continue

        try:
            agent_timeout = max(int(task.cursor_elapsed * 2) + 1, _AGENT_TIMEOUT_FLOOR)
            solution_label = f"challenger-{challenger.uid}"

            challenger_cfg = replace(_build_agent_config(config, challenger), agent_timeout=agent_timeout)
            chall_start = time.monotonic()
            solve_task_run(task_name=task.task_name, solution_name=solution_label, config=challenger_cfg)
            chall_elapsed = time.monotonic() - chall_start
            chall_timed_out = chall_elapsed >= agent_timeout

            if chall_timed_out:
                log.info("Duel %d: challenger uid=%s timed out on %s", duel_id, challenger.uid, task.task_name)

            chall_compare = compare_task_run(
                task_name=task.task_name, solution_names=["cursor", solution_label], config=config,
            )
            kc_compare = compare_task_run(
                task_name=task.task_name, solution_names=["king", solution_label], config=config,
            )

            c_lines = 0 if chall_timed_out else chall_compare.matched_changed_lines
            k_lines = task.king_lines

            if c_lines > k_lines:
                winner = "challenger"
            elif c_lines < k_lines:
                winner = "king"
            else:
                winner = "tie"

            result = ValidationRoundResult(
                task_name=task.task_name, winner=winner,
                king_lines=k_lines, challenger_lines=c_lines,
                king_similarity_ratio=task.king_similarity,
                challenger_similarity_ratio=0.0 if chall_timed_out else chall_compare.similarity_ratio,
                king_challenger_similarity=kc_compare.similarity_ratio,
                task_root=task.task_root,
                king_compare_root="", challenger_compare_root=chall_compare.comparison_root,
            )

            try:
                publish_round_data(duel_id=duel_id, task_name=task.task_name, tasks_root=config.tasks_root)
            except Exception:
                log.exception("R2 round publish failed (non-fatal)")

        except Exception as exc:
            result = ValidationRoundResult(
                task_name=task.task_name, winner="error",
                king_lines=0, challenger_lines=0,
                king_similarity_ratio=0.0, challenger_similarity_ratio=0.0,
                king_challenger_similarity=0.0,
                task_root=task.task_root, king_compare_root="", challenger_compare_root="",
                error=f"duel {duel_id} task {task.task_name} failed: {exc}",
            )

        rounds.append(result)
        if result.scored:
            scored += 1
            if result.winner == "challenger":
                wins += 1
            elif result.winner == "king":
                losses += 1
            else:
                ties += 1

            log.info("Duel %d round %d: %s (W=%d L=%d T=%d, need %d more wins)",
                     duel_id, scored, result.winner, wins, losses, ties, max(0, threshold - wins))

            # Early stop: threshold reached
            if wins >= threshold:
                log.info("Duel %d: challenger uid=%s WINS (%d/%d)", duel_id, challenger.uid, wins, scored)
                break
            # Early stop: can't reach threshold
            remaining = config.validate_duel_rounds - scored
            if wins + remaining < threshold:
                log.info("Duel %d: challenger uid=%s can't reach threshold (%d + %d < %d)",
                         duel_id, challenger.uid, wins, remaining, threshold)
                break

        if on_round_complete is not None:
            try:
                on_round_complete(duel_id=duel_id, wins=wins, losses=losses, ties=ties,
                                  scored=scored, threshold=threshold, rounds=rounds)
            except Exception:
                log.exception("on_round_complete callback failed (non-fatal)")

    # Determine outcome
    king_replaced = False
    dq_reason: str | None = None
    king_after = king

    if wins >= threshold:
        scored_sim = [r for r in rounds if r.scored and r.king_challenger_similarity > 0]
        mean_sim = sum(r.king_challenger_similarity for r in scored_sim) / len(scored_sim) if scored_sim else 0.0
        if mean_sim >= config.validate_copy_similarity_threshold:
            dq_reason = f"copy detected (similarity {mean_sim:.3f} >= {config.validate_copy_similarity_threshold})"
            log.warning("Duel %d: %s", duel_id, dq_reason)
        else:
            king_replaced = True

    return DuelResult(
        duel_id=duel_id, started_at=started_at, finished_at=_timestamp(),
        king_before=king, challenger=challenger, rounds=rounds,
        wins=wins, losses=losses, ties=ties,
        king_after=king_after, king_replaced=king_replaced,
        disqualification_reason=dq_reason,
    )


# ---------------------------------------------------------------------------
# Main validator loop
# ---------------------------------------------------------------------------

def validate_loop_run(config: RunConfig) -> ValidateStageResult:
    _setup_logging(debug=config.debug)
    threshold = config.validate_duel_rounds // 2 + config.validate_win_margin + 1
    log.info("Scoring: %d rounds, need %d wins to dethrone (margin=%d)",
             config.validate_duel_rounds, threshold, config.validate_win_margin)

    if (not config.validate_mock_set_weights
            and (not config.validate_wallet_name or not config.validate_wallet_hotkey)):
        raise ValueError("validate requires --wallet-name and --wallet-hotkey")

    paths = _prepare_validate_paths(config.validate_root)
    state = _load_state(paths.state_path)
    dashboard_history = _load_dashboard_history(paths.root / "dashboard_history.json")

    # Recover task index
    if config.tasks_root.exists():
        max_idx = 0
        for td in config.tasks_root.glob("validate-*"):
            parts = td.name.rsplit("-", 1)
            if len(parts) == 2 and parts[1].isdigit():
                max_idx = max(max_idx, int(parts[1]))
        if max_idx >= state.next_task_index:
            state.next_task_index = max_idx + 1

    pool = TaskPool(paths.pool_dir)
    pool_stop = threading.Event()
    validator_started_at = _timestamp()
    chain_data: dict[str, Any] | None = None

    github_client = _build_github_client(config)
    duel_count = 0

    # Active duels: hotkey -> (future, cancel_event, challenger)
    active_duels: dict[str, tuple[Future, threading.Event, ValidatorSubmission]] = {}
    executor = ThreadPoolExecutor(max_workers=config.validate_max_concurrency + 2)

    try:
        with _open_subtensor(config) as subtensor:
            log.info("Connected to chain for netuid %s", config.validate_netuid)

            # Initial chain poll + king setup
            chain_data = _fetch_chain_data(config.validate_netuid) or chain_data
            chain_submissions = _fetch_chain_submissions(subtensor=subtensor, github_client=github_client, config=config)
            _refresh_queue(chain_submissions=chain_submissions, config=config, state=state)
            _ensure_king(state=state)
            if state.current_king:
                if not state.king_since:
                    state.king_since = _timestamp()

            # Start pool filler
            executor.submit(_pool_filler_loop, config, state, pool, pool_stop)

            while True:
                # Poll chain
                current_block = subtensor.block
                log.info("Poll: block=%s king=%s queue=%d active_duels=%d pool=%d",
                         current_block,
                         state.current_king.commitment if state.current_king else None,
                         len(state.queue), len(active_duels), pool.size())

                chain_data = _fetch_chain_data(config.validate_netuid) or chain_data
                chain_submissions = _fetch_chain_submissions(subtensor=subtensor, github_client=github_client, config=config)
                _refresh_queue(chain_submissions=chain_submissions, config=config, state=state)

                prev_king = state.current_king.hotkey if state.current_king else None
                _ensure_king(state=state)
                if state.current_king and state.current_king.hotkey != prev_king:
                    state.king_since = _timestamp()
                    state.king_duels_defended = 0

                if state.current_king and len(state.current_king.commit_sha) < 40:
                    full = _resolve_public_commit(github_client, state.current_king.repo_full_name, state.current_king.commit_sha)
                    if full:
                        state.current_king.commit_sha = full

                if state.current_king:
                    _maybe_disqualify_king(subtensor=subtensor, github_client=github_client, config=config, state=state)
                    _maybe_set_weights(subtensor=subtensor, config=config, state=state, current_block=current_block)

                # Launch new duels up to concurrency limit
                while len(active_duels) < config.validate_max_concurrency and state.queue and state.current_king:
                    challenger = _pop_next_valid_challenger(subtensor=subtensor, github_client=github_client, config=config, state=state)
                    if challenger is None:
                        break

                    duel_id = state.next_duel_index
                    state.next_duel_index += 1
                    cancel_ev = threading.Event()

                    def make_callback(did: int) -> Any:
                        def cb(**kw: Any) -> None:
                            _publish_duel_progress(state, dashboard_history, config, validator_started_at, chain_data, active_duels, kw)
                        return cb

                    future = executor.submit(
                        _run_duel,
                        config=config, state=state, king=state.current_king,
                        challenger=challenger, duel_id=duel_id, pool=pool,
                        cancel_event=cancel_ev, on_round_complete=make_callback(duel_id),
                    )
                    active_duels[challenger.hotkey] = (future, cancel_ev, challenger)
                    log.info("Launched duel %d: uid=%s (%s)", duel_id, challenger.uid, challenger.repo_full_name)

                # Check completed duels
                king_changed = False
                for hotkey in list(active_duels.keys()):
                    future, cancel_ev, challenger = active_duels[hotkey]
                    if not future.done():
                        continue

                    duel_result = future.result()
                    del active_duels[hotkey]
                    duel_count += 1

                    log.info("Duel %d finished: uid=%s W=%d L=%d T=%d replaced=%s",
                             duel_result.duel_id, challenger.uid,
                             duel_result.wins, duel_result.losses, duel_result.ties,
                             duel_result.king_replaced)

                    if duel_result.king_replaced:
                        # Promote challenger
                        replacement = _resolve_promotion_candidate(
                            subtensor=subtensor, github_client=github_client,
                            config=config, state=state, primary_candidate=challenger,
                        )
                        if replacement:
                            _retire_hotkey(state, state.current_king.hotkey)
                            state.current_king = replacement
                            duel_result.king_after = replacement
                            king_changed = True
                            log.info("NEW KING: uid=%s (%s)", replacement.uid, replacement.agent_ref)

                            # Cancel all other duels, re-queue challengers
                            for other_hk in list(active_duels.keys()):
                                other_future, other_cancel, other_chall = active_duels[other_hk]
                                other_cancel.set()
                                state.queue.insert(0, other_chall)
                                log.info("Re-queued uid=%s (king changed)", other_chall.uid)
                            active_duels.clear()

                            # Flush pool (king solves are stale)
                            flushed = pool.flush()
                            log.info("Flushed %d pool tasks (new king)", flushed)
                    elif duel_result.disqualification_reason:
                        _mark_disqualified(state, challenger.hotkey)
                    else:
                        state.king_duels_defended += 1

                    # Persist duel
                    duel_dict = duel_result.to_dict()
                    _write_duel(paths, duel_result)
                    try:
                        publish_duel_data(duel_id=duel_result.duel_id, duel_dict=duel_dict)
                    except Exception:
                        log.exception("R2 duel publish failed (non-fatal)")
                    dashboard_history.append(duel_to_summary(duel_dict))
                    try:
                        publish_duel_index(duel_history=dashboard_history, latest_duel_dict=duel_dict)
                    except Exception:
                        log.exception("R2 index publish failed (non-fatal)")

                if king_changed:
                    state.king_since = _timestamp()
                    state.king_duels_defended = 0

                # Save and publish
                _save_state(paths.state_path, state)
                _save_dashboard_history(paths.root / "dashboard_history.json", dashboard_history)
                _publish_dashboard(state, dashboard_history, config, validator_started_at, active_duels, chain_data)
                _cleanup_old_tasks(config.tasks_root)
                _cleanup_orphaned_containers()

                if config.validate_max_duels is not None and duel_count >= config.validate_max_duels:
                    break

                time.sleep(config.validate_poll_interval_seconds)

    finally:
        pool_stop.set()
        for hk, (f, ev, c) in active_duels.items():
            ev.set()
        executor.shutdown(wait=False, cancel_futures=True)
        github_client.close()

    king = state.current_king
    if king is None:
        raise RuntimeError("validate loop exited without a current king")
    return ValidateStageResult(
        validate_root=str(paths.root), king_uid=king.uid,
        king_hotkey=king.hotkey, king_repo=king.agent_ref, duel_count=duel_count,
    )


# ---------------------------------------------------------------------------
# Dashboard publishing
# ---------------------------------------------------------------------------

def _publish_duel_progress(
    state: ValidatorState, history: list, config: RunConfig,
    started_at: str, chain_data: Any, active_duels: dict, round_info: dict,
) -> None:
    try:
        _publish_dashboard(state, history, config, started_at, active_duels, chain_data)
    except Exception:
        log.exception("Dashboard progress publish failed (non-fatal)")


def _publish_dashboard(
    state: ValidatorState, history: list[dict[str, Any]], config: RunConfig,
    validator_started_at: str, active_duels: Any,
    chain_data: dict[str, Any] | None = None,
) -> None:
    king = state.current_king
    king_dict = {
        "uid": king.uid, "hotkey": king.hotkey,
        "repo_full_name": king.repo_full_name,
        "repo_url": f"https://github.com/{king.repo_full_name}",
        "commit_sha": king.commit_sha,
    } if king else None

    threshold = config.validate_duel_rounds // 2 + config.validate_win_margin + 1
    active_duel_info: dict[str, Any] | None = None
    if isinstance(active_duels, dict) and active_duels:
        per_chall = {}
        for hk, (future, cancel_ev, chall) in active_duels.items():
            per_chall[hk] = {
                "uid": chall.uid, "repo": chall.repo_full_name,
                "commitment_block": chall.commitment_block,
                "running": not future.done(),
                "threshold": threshold,
                "duel_rounds": config.validate_duel_rounds,
            }
        active_duel_info = {
            "king_uid": king.uid if king else None,
            "king_repo": king.repo_full_name if king else None,
            "threshold": threshold,
            "duel_rounds": config.validate_duel_rounds,
            "per_challenger": per_chall,
        }

    commitment_map: dict[str, dict[str, Any]] = {}
    for d in history:
        for role in ("king", "challenger"):
            hk = d.get(f"{role}_hotkey")
            if hk and hk not in commitment_map:
                commitment_map[hk] = {"uid": d.get(f"{role}_uid"), "hotkey": hk, "repo": d.get(f"{role}_repo")}

    def _resolve_hk(hk: str) -> dict[str, Any]:
        if hk in commitment_map:
            return commitment_map[hk]
        c = state.locked_commitments.get(hk, "")
        repo = c.split("@")[0] if "@" in c else c
        return {"uid": None, "hotkey": hk, "repo": repo or "unknown"}

    total_rounds = sum(len(d.get("rounds", [])) for d in history)
    status = {
        "validator_started_at": validator_started_at,
        "netuid": config.validate_netuid,
        "scoring": {
            "method": "majority",
            "duel_rounds": config.validate_duel_rounds,
            "win_margin": config.validate_win_margin,
            "threshold": threshold,
        },
        "queue": [{"uid": s.uid, "repo": s.repo_full_name, "hotkey": s.hotkey, "commitment_block": s.commitment_block} for s in state.queue],
        "active_duel": active_duel_info,
        "disqualified": [_resolve_hk(hk) for hk in state.disqualified_hotkeys],
        "retired": [_resolve_hk(hk) for hk in state.retired_hotkeys],
        "total_rounds": total_rounds,
        "miners_seen": len(state.seen_hotkeys),
        "king_since": state.king_since,
        "king_duels_defended": state.king_duels_defended,
        "chain_data": chain_data,
    }

    payload = {"updated_at": _timestamp(), "current_king": king_dict, "duels": history, "status": status}
    try:
        write_json(config.validate_root / "dashboard_data.json", payload)
    except Exception:
        log.exception("Local dashboard write failed (non-fatal)")
    try:
        publish_dashboard_data(current_king=king_dict, duel_history=history, status=status)
    except Exception:
        log.exception("R2 dashboard publish failed (non-fatal)")


# ---------------------------------------------------------------------------
# Chain + queue management (preserved from original)
# ---------------------------------------------------------------------------

def _build_github_client(config: RunConfig) -> httpx.Client:
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "swe-eval-validate"}
    if config.github_token:
        headers["Authorization"] = f"Bearer {config.github_token}"
    return httpx.Client(base_url="https://api.github.com", headers=headers, follow_redirects=True, timeout=config.http_timeout)


def _refresh_queue(*, chain_submissions: list[ValidatorSubmission], config: RunConfig, state: ValidatorState) -> None:
    known = set(state.seen_hotkeys)
    if state.current_king:
        known.add(state.current_king.hotkey)
    known.update(s.hotkey for s in state.queue)
    for sub in chain_submissions:
        locked = state.locked_commitments.get(sub.hotkey)
        if locked is not None and locked != sub.commitment:
            log.warning("Hotkey %s changed commitment; ignoring (immutable)", sub.hotkey)
            continue
        if sub.hotkey in known:
            continue
        if config.validate_queue_size is not None and len(state.queue) >= config.validate_queue_size:
            break
        state.locked_commitments[sub.hotkey] = sub.commitment
        state.queue.append(sub)
        state.seen_hotkeys.append(sub.hotkey)
        known.add(sub.hotkey)
    state.queue.sort(key=lambda s: (s.commitment_block, s.uid, s.hotkey))


def _fetch_chain_submissions(*, subtensor, github_client: httpx.Client, config: RunConfig) -> list[ValidatorSubmission]:
    if config.validate_mock_local_agent:
        return _mock_submissions(subtensor=subtensor, config=config)
    revealed = subtensor.commitments.get_all_revealed_commitments(config.validate_netuid)
    current_commitments = subtensor.commitments.get_all_commitments(config.validate_netuid)
    submissions: list[ValidatorSubmission] = []
    seen: set[str] = set()
    current_block = subtensor.block

    for hotkey, entries in revealed.items():
        normalized = [(int(i[0]), str(i[1])) for i in entries if isinstance(i, tuple) and len(i) == 2] if isinstance(entries, tuple) else []
        if not normalized:
            continue
        block, commitment = min(normalized, key=lambda x: x[0])
        sub = _build_submission(subtensor=subtensor, github_client=github_client, config=config, hotkey=str(hotkey), commitment=str(commitment), commitment_block=block)
        if sub:
            submissions.append(sub)
            seen.add(sub.hotkey)

    for hotkey, commitment in current_commitments.items():
        hotkey = str(hotkey)
        if hotkey in seen:
            continue
        commit_block = current_block
        try:
            meta = subtensor.commitments.get_commitment_metadata(config.validate_netuid, hotkey)
            if isinstance(meta, list):
                blocks = [int(m["block"]) for m in meta if isinstance(m, dict) and "block" in m]
                if blocks:
                    commit_block = min(blocks)
            elif isinstance(meta, dict) and "block" in meta:
                commit_block = int(meta["block"])
        except Exception:
            pass
        sub = _build_submission(subtensor=subtensor, github_client=github_client, config=config, hotkey=hotkey, commitment=str(commitment), commitment_block=commit_block)
        if sub:
            submissions.append(sub)

    submissions.sort(key=lambda s: (s.commitment_block, s.uid, s.hotkey))
    return submissions


def _mock_submissions(*, subtensor, config: RunConfig) -> list[ValidatorSubmission]:
    if not config.validate_mock_local_agent:
        return []
    p = str(Path(config.validate_mock_local_agent).expanduser().resolve())
    b = subtensor.block
    return [
        ValidatorSubmission(hotkey="mock-king-hotkey", uid=1, repo_full_name="local/mock-agent", repo_url=p, commit_sha="local-king", commitment="local/mock-agent@local-king", commitment_block=b-1, local_path=p),
        ValidatorSubmission(hotkey="mock-challenger-hotkey", uid=2, repo_full_name="local/mock-agent", repo_url=p, commit_sha="local-challenger", commitment="local/mock-agent@local-challenger", commitment_block=b, local_path=p),
    ]


def _build_submission(*, subtensor, github_client, config, hotkey, commitment, commitment_block) -> ValidatorSubmission | None:
    parsed = _parse_submission_commitment(commitment)
    if not parsed:
        return None
    uid = subtensor.subnets.get_uid_for_hotkey_on_subnet(hotkey, config.validate_netuid)
    if uid is None:
        return None
    repo, sha = parsed
    full_sha = _resolve_public_commit(github_client, repo, sha)
    if not full_sha:
        return None
    return ValidatorSubmission(hotkey=hotkey, uid=int(uid), repo_full_name=repo, repo_url=f"https://github.com/{repo}.git", commit_sha=full_sha, commitment=commitment, commitment_block=commitment_block)


def _ensure_king(*, state: ValidatorState) -> None:
    if state.current_king or not state.queue:
        return
    state.current_king = state.queue.pop(0)


def _pop_next_valid_challenger(*, subtensor, github_client, config, state) -> ValidatorSubmission | None:
    while state.queue:
        c = state.queue.pop(0)
        if _submission_is_eligible(subtensor=subtensor, github_client=github_client, config=config, submission=c):
            return c
        _mark_disqualified(state, c.hotkey)
    return None


def _submission_is_eligible(*, subtensor, github_client, config, submission) -> bool:
    uid = subtensor.subnets.get_uid_for_hotkey_on_subnet(submission.hotkey, config.validate_netuid)
    if uid is None:
        return False
    if submission.local_path is not None:
        submission.uid = int(uid)
        return True
    if not _is_public_commit(github_client, submission.repo_full_name, submission.commit_sha):
        return False
    submission.uid = int(uid)
    return True


def _maybe_disqualify_king(*, subtensor, github_client, config, state) -> None:
    king = state.current_king
    if not king:
        return
    if _submission_is_eligible(subtensor=subtensor, github_client=github_client, config=config, submission=king):
        return
    _mark_disqualified(state, king.hotkey)
    state.current_king = None
    state.current_king = _pop_next_valid_challenger(subtensor=subtensor, github_client=github_client, config=config, state=state)


def _retire_hotkey(state, hotkey):
    if hotkey not in state.retired_hotkeys:
        state.retired_hotkeys.append(hotkey)

def _mark_disqualified(state, hotkey):
    if hotkey not in state.disqualified_hotkeys:
        state.disqualified_hotkeys.append(hotkey)

def _resolve_promotion_candidate(*, subtensor, github_client, config, state, primary_candidate):
    if _submission_is_eligible(subtensor=subtensor, github_client=github_client, config=config, submission=primary_candidate):
        return primary_candidate
    _mark_disqualified(state, primary_candidate.hotkey)
    return _pop_next_valid_challenger(subtensor=subtensor, github_client=github_client, config=config, state=state)


# ---------------------------------------------------------------------------
# Weight setting
# ---------------------------------------------------------------------------

def _maybe_set_weights(*, subtensor, config, state, current_block):
    king = state.current_king
    if not king:
        return
    if state.last_weight_block is not None and current_block - state.last_weight_block < config.validate_weight_interval_blocks:
        return
    neurons = list(subtensor.neurons.neurons_lite(config.validate_netuid))
    if not neurons:
        raise RuntimeError(f"Subnet {config.validate_netuid} has no neurons")
    uid = subtensor.subnets.get_uid_for_hotkey_on_subnet(king.hotkey, config.validate_netuid)
    if uid is None:
        raise RuntimeError(f"King {king.hotkey} is no longer registered")
    king.uid = int(uid)
    uids = [int(n.uid) for n in neurons]
    weights = [1.0 if u == king.uid else 0.0 for u in uids]
    if config.validate_mock_set_weights:
        state.last_weight_block = current_block
        log.info("Mocked set_weights at block %s to king uid=%s", current_block, king.uid)
        return
    wallet = bt.Wallet(name=config.validate_wallet_name, hotkey=config.validate_wallet_hotkey, path=config.validate_wallet_path)
    resp = subtensor.extrinsics.set_weights(wallet=wallet, netuid=config.validate_netuid, uids=uids, weights=weights, wait_for_inclusion=True, wait_for_finalization=True)
    state.last_weight_block = current_block
    log.info("Set weights at block %s to king uid=%s response=%s", current_block, king.uid, resp)


# ---------------------------------------------------------------------------
# Config builders
# ---------------------------------------------------------------------------

def _build_cursor_config(config: RunConfig) -> RunConfig:
    return replace(config, solver_backend="cursor", solve_agent="cursor", solver_agent_source=None, solver_model=config.solver_model or _CURSOR_MODEL_FOR_SONNET4)

def _build_agent_config(config: RunConfig, sub: ValidatorSubmission) -> RunConfig:
    if sub.local_path:
        src = SolverAgentSource(raw=sub.local_path, kind="local_path", local_path=sub.local_path)
        return replace(config, solver_backend="docker-pi", solve_agent=sub.local_path, solver_agent_source=src)
    src = SolverAgentSource(raw=sub.agent_ref, kind="github_repo", repo_url=sub.repo_url, agent_subdir=_DEFAULT_GITHUB_AGENT_SUBDIR, commit_sha=sub.commit_sha)
    return replace(config, solver_backend="docker-pi", solve_agent=sub.agent_ref, solver_agent_source=src)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _allocate_task_name(state: ValidatorState) -> str:
    idx = state.next_task_index
    state.next_task_index += 1
    ts = datetime.now(tz=UTC).strftime("%Y%m%d%H%M%S")
    return f"validate-{ts}-{idx:06d}"

def _prepare_validate_paths(root: Path) -> ValidatePaths:
    root.mkdir(parents=True, exist_ok=True)
    duels = root / "duels"
    duels.mkdir(parents=True, exist_ok=True)
    pool = root / "task-pool"
    pool.mkdir(parents=True, exist_ok=True)
    return ValidatePaths(root=root, state_path=root / "state.json", duels_dir=duels, pool_dir=pool)

def _load_state(path: Path) -> ValidatorState:
    if not path.exists():
        return ValidatorState()
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise RuntimeError(f"Invalid state file: {path}")
    return ValidatorState.from_dict(payload)

def _save_state(path: Path, state: ValidatorState) -> None:
    write_json(path, state.to_dict())

def _write_duel(paths: ValidatePaths, duel: DuelResult) -> None:
    write_json(paths.duels_dir / f"{duel.duel_id:06d}.json", duel.to_dict())

def _load_dashboard_history(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text())
        return payload if isinstance(payload, list) else []
    except Exception:
        log.exception("Failed to load dashboard history; starting fresh")
        return []

def _save_dashboard_history(path: Path, history: list) -> None:
    write_json(path, history)


# ---------------------------------------------------------------------------
# Commitment parsing + GitHub helpers
# ---------------------------------------------------------------------------

def _parse_submission_commitment(raw: str) -> tuple[str, str] | None:
    cleaned = raw.strip().rstrip("/")
    m = _GITHUB_COMMIT_RE.fullmatch(cleaned)
    if m:
        return m.group("repo"), m.group("sha")
    for prefix in ("https://github.com/", "github.com/"):
        if cleaned.startswith(prefix):
            parts = [p for p in cleaned[len(prefix):].split("/") if p]
            if len(parts) >= 4 and parts[2] == "commit":
                return "/".join(parts[:2]), parts[3]
    return None

def _resolve_public_commit(client: httpx.Client, repo: str, sha: str) -> str | None:
    r = client.get(f"/repos/{repo}")
    if r.status_code != 200 or r.json().get("private") is not False:
        return None
    r2 = client.get(f"/repos/{repo}/commits/{sha}")
    return r2.json().get("sha", sha) if r2.status_code == 200 else None

def _is_public_commit(client: httpx.Client, repo: str, sha: str) -> bool:
    return _resolve_public_commit(client, repo, sha) is not None


# ---------------------------------------------------------------------------
# Chain connection + market data
# ---------------------------------------------------------------------------

def _open_subtensor(config: RunConfig):
    if config.validate_mock_local_agent:
        return _MockSubtensorApi()
    network = config.validate_subtensor_endpoint or config.validate_network
    if network:
        return bt.SubtensorApi(network=network, websocket_shutdown_timer=0)
    return bt.SubtensorApi(websocket_shutdown_timer=0)


def _fetch_chain_data(netuid: int) -> dict[str, Any] | None:
    api_key = os.environ.get("TMC_API_KEY")
    if not api_key:
        return None
    headers = {"Authorization": api_key, "Accept": "application/json"}
    base = "https://api.taomarketcap.com/public/v1"
    try:
        with httpx.Client(timeout=15, headers=headers) as c:
            market = c.get(f"{base}/market/market-data/")
            subnet = c.get(f"{base}/subnets/{netuid}/")
            weights = c.get(f"{base}/subnets/weights/{netuid}/")
        m = market.json() if market.status_code == 200 else {}
        s = subnet.json() if subnet.status_code == 200 else {}
        w = weights.json() if weights.status_code == 200 else {}
        snap = s.get("latest_snapshot", {})
        burn = int(snap.get("burn", 0))
        tao = float(m.get("current_price", 0))
        alpha_tao = float(snap.get("subnet_moving_price", 0))
        wt = []
        for we in w.get("weights", []):
            for tid, val in we.get("value", {}).items():
                wt.append({"validator_uid": we["uid"], "miner_uid": int(tid), "weight": val})
        return {
            "fetched_at": datetime.now(tz=UTC).isoformat(),
            "tao_price_usd": tao, "tao_change_24h": float((m.get("usd_quote") or {}).get("percent_change_24h", 0)),
            "tao_market_cap": float((m.get("usd_quote") or {}).get("market_cap", 0)),
            "alpha_price_tao": alpha_tao, "alpha_price_usd": alpha_tao * tao,
            "subnet_tao": int(snap.get("subnet_tao", 0)) / 1e9,
            "subnet_emission_per_day": int(snap.get("subnet_tao_in_emission", 0)) / 1e9 * 7200,
            "burn_cost_rao": burn, "burn_cost_tao": burn / 1e9, "burn_cost_usd": burn / 1e9 * tao,
            "neuron_count": int(snap.get("subnetwork_n", 0)), "max_neurons": int(snap.get("max_allowed_uids", 256)),
            "token_symbol": snap.get("token_symbol", ""),
            "subnet_name": (snap.get("subnet_identities_v3") or {}).get("subnetName", ""),
            "tempo": int(snap.get("tempo", 0)), "immunity_period": int(snap.get("immunity_period", 0)),
            "weights": wt,
        }
    except Exception:
        log.exception("Failed to fetch chain data (non-fatal)")
        return None


# ---------------------------------------------------------------------------
# Cleanup utilities
# ---------------------------------------------------------------------------

def _cleanup_old_tasks(tasks_root: Path, keep: int = 30) -> None:
    try:
        dirs = sorted(tasks_root.glob("validate-*"), key=lambda p: p.name)
        if len(dirs) <= keep:
            return
        for d in dirs[:-keep]:
            shutil.rmtree(d, ignore_errors=True)
            log.info("Cleaned task dir: %s", d.name)
    except Exception:
        log.exception("Task cleanup failed (non-fatal)")

def _cleanup_orphaned_containers(max_age: int = 7200) -> None:
    try:
        r = subprocess.run(["docker", "ps", "-q", "--filter", "name=swe-eval-"], capture_output=True, text=True, timeout=10)
        if r.returncode != 0 or not r.stdout.strip():
            return
        for cid in r.stdout.strip().splitlines():
            ir = subprocess.run(["docker", "inspect", "--format", "{{.State.StartedAt}}", cid], capture_output=True, text=True, timeout=10)
            if ir.returncode != 0:
                continue
            started = datetime.fromisoformat(ir.stdout.strip().replace("Z", "+00:00"))
            age = (datetime.now(tz=UTC) - started).total_seconds()
            if age > max_age:
                subprocess.run(["docker", "kill", cid], capture_output=True, timeout=10)
                subprocess.run(["docker", "rm", "-f", cid], capture_output=True, timeout=10)
                log.info("Killed orphaned container %s (age %.0fs)", cid[:12], age)
    except Exception:
        log.exception("Container cleanup failed (non-fatal)")

def _count_patch_lines(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text().splitlines() if line.startswith(("+", "-")) and not line.startswith(("+++", "---")))

def _timestamp() -> str:
    return datetime.now(tz=UTC).isoformat()
