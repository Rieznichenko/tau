from __future__ import annotations

import json
import logging
import os
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3

log = logging.getLogger("swe-eval.r2")

_R2_KEY_PREFIX = "sn66/"
_DASHBOARD_KEY = f"{_R2_KEY_PREFIX}dashboard.json"
_DUELS_PREFIX = f"{_R2_KEY_PREFIX}duels/"
_INDEX_KEY = f"{_DUELS_PREFIX}index.json"

_client_lock = threading.Lock()
_cached_client = None
_client_resolved = False


def _get_s3_client():
    """Return a cached boto3 S3 client, or None if credentials are missing."""
    global _cached_client, _client_resolved  # noqa: PLW0603
    if _client_resolved:
        return _cached_client
    with _client_lock:
        if _client_resolved:
            return _cached_client
        endpoint = os.environ.get("R2_URL")
        access_key = os.environ.get("R2_ACCESS_KEY_ID")
        secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
        if not all([endpoint, access_key, secret_key]):
            _cached_client = None
        else:
            _cached_client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name="auto",
            )
        _client_resolved = True
        return _cached_client


def _get_bucket() -> str:
    return os.environ.get("R2_BUCKET_NAME", "constantinople")


def _upload_json(key: str, data: Any) -> bool:
    """Upload a JSON-serializable object to R2. Returns True on success."""
    client = _get_s3_client()
    if client is None:
        return False
    body = json.dumps(data, indent=2)
    client.put_object(
        Bucket=_get_bucket(),
        Key=key,
        Body=body.encode(),
        ContentType="application/json",
    )
    return True


def _upload_text(key: str, content: str, content_type: str = "text/plain") -> bool:
    """Upload text content to R2. Returns True on success."""
    client = _get_s3_client()
    if client is None:
        return False
    client.put_object(
        Bucket=_get_bucket(),
        Key=key,
        Body=content.encode(),
        ContentType=content_type,
    )
    return True


def publish_dashboard_data(
    *,
    current_king: dict[str, Any] | None,
    duel_history: list[dict[str, Any]],
    status: dict[str, Any] | None = None,
) -> bool:
    """Serialize and upload dashboard.json to R2. Returns True on success."""
    if _get_s3_client() is None:
        log.warning("R2 credentials not configured; skipping dashboard publish")
        return False

    payload = {
        "updated_at": datetime.now(tz=UTC).isoformat(),
        "current_king": current_king,
        "duels": duel_history,
        "status": status,
    }
    try:
        _upload_json(_DASHBOARD_KEY, payload)
        log.info("Published dashboard data to r2://%s/%s (%d duels)", _get_bucket(), _DASHBOARD_KEY, len(duel_history))
        return True
    except Exception:
        log.exception("Failed to publish dashboard data to R2")
        return False


def duel_to_summary(duel_dict: dict[str, Any]) -> dict[str, Any]:
    """Extract the fields the dashboard needs from a full DuelResult dict."""
    king_before = duel_dict.get("king_before", {})
    challenger = duel_dict.get("challenger", {})
    rounds = duel_dict.get("rounds", [])

    scored_rounds = [r for r in rounds if r.get("error") is None]
    king_ratios = [r["king_similarity_ratio"] for r in scored_rounds if "king_similarity_ratio" in r]
    challenger_ratios = [r["challenger_similarity_ratio"] for r in scored_rounds if "challenger_similarity_ratio" in r]

    return {
        "duel_id": duel_dict.get("duel_id"),
        "started_at": duel_dict.get("started_at"),
        "finished_at": duel_dict.get("finished_at"),
        "king_uid": king_before.get("uid"),
        "king_hotkey": king_before.get("hotkey"),
        "king_repo": king_before.get("repo_full_name"),
        "king_repo_url": f"https://github.com/{king_before.get('repo_full_name', '')}",
        "king_commit_sha": king_before.get("commit_sha"),
        "king_commitment_block": king_before.get("commitment_block"),
        "challenger_uid": challenger.get("uid"),
        "challenger_hotkey": challenger.get("hotkey"),
        "challenger_repo": challenger.get("repo_full_name"),
        "challenger_repo_url": f"https://github.com/{challenger.get('repo_full_name', '')}",
        "challenger_commit_sha": challenger.get("commit_sha"),
        "challenger_commitment_block": challenger.get("commitment_block"),
        "king_similarity_ratio_mean": (sum(king_ratios) / len(king_ratios)) if king_ratios else 0.0,
        "challenger_similarity_ratio_mean": (sum(challenger_ratios) / len(challenger_ratios)) if challenger_ratios else 0.0,
        "wins": duel_dict.get("wins", 0),
        "losses": duel_dict.get("losses", 0),
        "ties": duel_dict.get("ties", 0),
        "king_replaced": duel_dict.get("king_replaced", False),
        "disqualification_reason": duel_dict.get("disqualification_reason"),
        "rounds": [
            {
                "task_name": r.get("task_name"),
                "winner": r.get("winner"),
                "king_similarity_ratio": r.get("king_similarity_ratio", 0.0),
                "challenger_similarity_ratio": r.get("challenger_similarity_ratio", 0.0),
                "king_challenger_similarity": r.get("king_challenger_similarity", 0.0),
                "king_lines": r.get("king_lines", 0),
                "challenger_lines": r.get("challenger_lines", 0),
                "cursor_lines": r.get("cursor_lines", 0),
            }
            for r in scored_rounds
        ],
    }


def _duel_key_prefix(duel_id: int) -> str:
    return f"{_DUELS_PREFIX}{duel_id:06d}/"


def _round_key_prefix(duel_id: int, task_name: str) -> str:
    return f"{_duel_key_prefix(duel_id)}rounds/{task_name}/"


def publish_round_data(
    *,
    duel_id: int,
    task_name: str,
    tasks_root: Path,
) -> bool:
    """Upload all artifacts for a single validation round to R2.

    Reads task.json, reference.patch, solution diffs, and comparison JSONs
    from the local task workspace and uploads them under:
        sn66/duels/{duel_id}/rounds/{task_name}/...

    Returns True if at least one file was uploaded, False otherwise.
    """
    if _get_s3_client() is None:
        return False

    from workspace import build_compare_paths, build_solution_paths, build_task_paths

    prefix = _round_key_prefix(duel_id, task_name)
    task_paths = build_task_paths(tasks_root, task_name)
    uploaded = 0

    def _try_upload_json_file(local_path: Path, r2_key: str) -> None:
        nonlocal uploaded
        if not local_path.exists():
            return
        try:
            data = json.loads(local_path.read_text())
            _upload_json(r2_key, data)
            uploaded += 1
        except Exception:
            log.exception("Failed to upload %s to R2 (non-fatal)", r2_key)

    def _try_upload_text_file(local_path: Path, r2_key: str, content_type: str = "text/plain") -> None:
        nonlocal uploaded
        if not local_path.exists():
            return
        try:
            _upload_text(r2_key, local_path.read_text(), content_type)
            uploaded += 1
        except Exception:
            log.exception("Failed to upload %s to R2 (non-fatal)", r2_key)

    _try_upload_json_file(task_paths.task_json_path, f"{prefix}task.json")
    _try_upload_text_file(task_paths.reference_patch_path, f"{prefix}reference.patch", "text/x-diff")

    for solution_name in ("cursor", "king", "challenger"):
        sol_paths = build_solution_paths(task_paths, solution_name)
        _try_upload_text_file(
            sol_paths.solution_diff_path,
            f"{prefix}solutions/{solution_name}.diff",
            "text/x-diff",
        )

    for compare_pair in (
        ["cursor", "king"],
        ["cursor", "challenger"],
        ["king", "challenger"],
    ):
        cmp_name = f"{compare_pair[0]}--vs--{compare_pair[1]}"
        cmp_paths = build_compare_paths(task_paths, cmp_name)
        _try_upload_json_file(
            cmp_paths.compare_json_path,
            f"{prefix}comparisons/{cmp_name}.json",
        )

    log.info(
        "Published %d round artifacts for duel %d task %s to R2",
        uploaded, duel_id, task_name,
    )
    return uploaded > 0


def publish_duel_data(*, duel_id: int, duel_dict: dict[str, Any]) -> bool:
    """Upload the full DuelResult JSON to R2.

    Writes to: sn66/duels/{duel_id}/duel.json
    """
    if _get_s3_client() is None:
        return False
    key = f"{_duel_key_prefix(duel_id)}duel.json"
    try:
        _upload_json(key, duel_dict)
        log.info("Published duel %d to r2://%s/%s", duel_id, _get_bucket(), key)
        return True
    except Exception:
        log.exception("Failed to publish duel %d to R2 (non-fatal)", duel_id)
        return False


def publish_duel_index(
    *,
    duel_history: list[dict[str, Any]],
    latest_duel_dict: dict[str, Any] | None = None,
) -> bool:
    """Rebuild and upload sn66/duels/index.json from the dashboard history.

    Each entry contains enough metadata for discovery plus the list of
    round task names so consumers can construct full key paths.
    """
    if _get_s3_client() is None:
        return False

    public_base_url = os.environ.get("R2_PUBLIC_URL", "")
    entries: list[dict[str, Any]] = []

    round_names_by_duel: dict[int, list[str]] = {}
    if latest_duel_dict:
        did = latest_duel_dict.get("duel_id")
        if did is not None:
            round_names_by_duel[did] = [
                r.get("task_name", "") for r in latest_duel_dict.get("rounds", [])
            ]

    for summary in duel_history:
        duel_id = summary.get("duel_id")
        if duel_id is None:
            continue
        round_task_names = round_names_by_duel.get(
            duel_id,
            [r.get("task_name", "") for r in summary.get("rounds", [])],
        )
        entries.append({
            "duel_id": duel_id,
            "started_at": summary.get("started_at"),
            "finished_at": summary.get("finished_at"),
            "king_repo": summary.get("king_repo"),
            "challenger_repo": summary.get("challenger_repo"),
            "king_replaced": summary.get("king_replaced", False),
            "wins": summary.get("wins", 0),
            "losses": summary.get("losses", 0),
            "ties": summary.get("ties", 0),
            "rounds": round_task_names,
            "path": f"{_DUELS_PREFIX}{duel_id:06d}/",
        })

    payload = {
        "updated_at": datetime.now(tz=UTC).isoformat(),
        "public_base_url": public_base_url,
        "duels": entries,
    }
    try:
        _upload_json(_INDEX_KEY, payload)
        log.info("Published duel index (%d entries) to R2", len(entries))
        return True
    except Exception:
        log.exception("Failed to publish duel index to R2 (non-fatal)")
        return False


def backfill_duel_to_r2(
    duel_json_path: Path,
    tasks_root: Path,
) -> bool:
    """Upload a historical duel and its round artifacts to R2.

    Reads the full duel JSON from disk, uploads the duel record, then
    iterates over rounds and uploads each round's artifacts if available.
    Returns True if the duel record was uploaded.
    """
    if _get_s3_client() is None:
        log.warning("R2 credentials not configured; skipping backfill")
        return False

    duel_dict = json.loads(duel_json_path.read_text())
    duel_id = duel_dict["duel_id"]

    if not publish_duel_data(duel_id=duel_id, duel_dict=duel_dict):
        return False

    for round_data in duel_dict.get("rounds", []):
        task_name = round_data.get("task_name")
        if not task_name:
            continue
        try:
            publish_round_data(duel_id=duel_id, task_name=task_name, tasks_root=tasks_root)
        except Exception:
            log.exception(
                "Backfill: failed to upload round %s for duel %d (non-fatal)",
                task_name, duel_id,
            )

    log.info("Backfilled duel %d from %s", duel_id, duel_json_path)
    return True
