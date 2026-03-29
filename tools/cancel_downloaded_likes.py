import argparse
import asyncio
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from config import ConfigLoader, DEFAULT_DATABASE_FILENAME
from core.api_client import DouyinAPIClient

DEFAULT_CONFIG_PATH = Path("config.yml")
DEFAULT_MANIFEST_PATH = Path("Downloaded/download_manifest.jsonl")
DEFAULT_BATCH_SCOPE = "latest"
DEFAULT_BATCH_GAP_SECONDS = 900


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel likes for aweme entries that were already downloaded."
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Config file path (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help=(
            "SQLite database path "
            f"(default: <download_path>/{DEFAULT_DATABASE_FILENAME})"
        ),
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST_PATH,
        help=f"Download manifest path (default: {DEFAULT_MANIFEST_PATH})",
    )
    parser.add_argument(
        "--source",
        choices=["auto", "db", "manifest"],
        default="auto",
        help="Where to read aweme ids from (default: auto)",
    )
    parser.add_argument(
        "--batch-scope",
        choices=["latest", "all"],
        default=DEFAULT_BATCH_SCOPE,
        help="Which downloaded batch to unlike (default: latest)",
    )
    parser.add_argument(
        "--batch-gap-seconds",
        type=int,
        default=DEFAULT_BATCH_GAP_SECONDS,
        help=(
            "Gap threshold used to split DB download batches when "
            "--batch-scope=latest (default: 900)"
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max aweme count to process",
    )
    parser.add_argument(
        "--aweme-id",
        action="append",
        dest="aweme_ids",
        help="Specific aweme_id to unlike. Can be passed multiple times.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser headless for like cleanup",
    )
    parser.add_argument(
        "--profile-dir",
        help="Persistent Playwright profile directory for saved login state",
    )
    parser.add_argument(
        "--wait-timeout-seconds",
        type=int,
        help="Override login/verification wait timeout",
    )
    parser.add_argument(
        "--request-interval-ms",
        type=int,
        help="Override delay between unlike requests",
    )
    return parser.parse_args(argv)


def as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def dedupe_aweme_ids(aweme_ids: List[str]) -> List[str]:
    normalized: List[str] = []
    seen: set[str] = set()
    for aweme_id in aweme_ids:
        aweme_id_str = str(aweme_id or "").strip()
        if not aweme_id_str or aweme_id_str in seen:
            continue
        seen.add(aweme_id_str)
        normalized.append(aweme_id_str)
    return normalized


def _select_latest_db_batch(
    rows: List[tuple[str, int]], batch_gap_seconds: int
) -> List[tuple[str, int]]:
    if not rows:
        return []

    normalized_gap = max(1, int(batch_gap_seconds or 0))
    batch_rows: List[tuple[str, int]] = []
    previous_ts: Optional[int] = None
    for aweme_id, download_time in rows:
        aweme_id_str = str(aweme_id or "").strip()
        download_ts = int(download_time or 0)
        if not aweme_id_str or download_ts <= 0:
            continue
        if previous_ts is not None and previous_ts - download_ts > normalized_gap:
            break
        batch_rows.append((aweme_id_str, download_ts))
        previous_ts = download_ts
    return batch_rows


def collect_aweme_ids_from_db(
    db_path: Path,
    *,
    batch_scope: str = "all",
    batch_gap_seconds: int = DEFAULT_BATCH_GAP_SECONDS,
) -> List[str]:
    if not db_path.exists():
        return []

    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(
            """
            SELECT aweme_id
                , download_time
            FROM aweme
            WHERE aweme_id IS NOT NULL AND aweme_id != ''
            ORDER BY download_time DESC, id DESC
            """
        )
        rows = [
            (str(row[0]), int(row[1] or 0))
            for row in cursor.fetchall()
            if row and row[0]
        ]
        if batch_scope == "latest":
            rows = _select_latest_db_batch(rows, batch_gap_seconds=batch_gap_seconds)
        return dedupe_aweme_ids([row[0] for row in rows if row and row[0]])
    finally:
        conn.close()


def collect_aweme_ids_from_manifest(manifest_path: Path) -> List[str]:
    if not manifest_path.exists():
        return []

    aweme_ids: List[str] = []
    for raw_line in manifest_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        aweme_id = record.get("aweme_id")
        if aweme_id:
            aweme_ids.append(str(aweme_id))
    return dedupe_aweme_ids(aweme_ids)


def collect_downloaded_aweme_ids(
    db_path: Path,
    manifest_path: Path,
    source: str = "auto",
    *,
    batch_scope: str = DEFAULT_BATCH_SCOPE,
    batch_gap_seconds: int = DEFAULT_BATCH_GAP_SECONDS,
) -> List[str]:
    if source == "db":
        return collect_aweme_ids_from_db(
            db_path,
            batch_scope=batch_scope,
            batch_gap_seconds=batch_gap_seconds,
        )
    if source == "manifest":
        return collect_aweme_ids_from_manifest(manifest_path)

    db_ids = collect_aweme_ids_from_db(
        db_path,
        batch_scope=batch_scope,
        batch_gap_seconds=batch_gap_seconds,
    )
    if batch_scope == "latest" and db_ids:
        return db_ids

    combined = list(db_ids)
    if manifest_path.exists():
        combined.extend(collect_aweme_ids_from_manifest(manifest_path))
    return dedupe_aweme_ids(combined)


async def main_async(args: argparse.Namespace) -> int:
    if args.config and not args.config.exists():
        print(f"[ERROR] Config file not found: {args.config}", file=sys.stderr)
        return 1

    config = ConfigLoader(str(args.config)) if args.config else ConfigLoader()
    db_path = args.db if args.db is not None else config.get_database_path()
    cookies = config.get_cookies()
    if not cookies:
        print("[ERROR] No cookies found in config.", file=sys.stderr)
        return 1

    cleanup_cfg = config.get("like_cleanup", {}) or {}
    if not isinstance(cleanup_cfg, dict):
        cleanup_cfg = {"enabled": cleanup_cfg}

    explicit_aweme_ids = dedupe_aweme_ids(list(args.aweme_ids or []))
    batch_scope = str(
        cleanup_cfg.get("batch_scope", args.batch_scope) or args.batch_scope
    ).strip() or DEFAULT_BATCH_SCOPE
    if batch_scope not in {"latest", "all"}:
        batch_scope = DEFAULT_BATCH_SCOPE
    batch_gap_seconds = int(
        cleanup_cfg.get("batch_gap_seconds", args.batch_gap_seconds)
        if cleanup_cfg.get("batch_gap_seconds") is not None
        else args.batch_gap_seconds
    )
    aweme_ids = (
        explicit_aweme_ids
        if explicit_aweme_ids
        else collect_downloaded_aweme_ids(
            db_path,
            args.manifest,
            args.source,
            batch_scope=batch_scope,
            batch_gap_seconds=batch_gap_seconds,
        )
    )
    if args.limit and args.limit > 0:
        aweme_ids = aweme_ids[: args.limit]

    if not aweme_ids:
        print("[INFO] No downloaded aweme ids found.")
        return 0

    headless = args.headless or as_bool(cleanup_cfg.get("headless"), default=False)
    persist_login = as_bool(cleanup_cfg.get("persist_login"), default=True)
    profile_dir = str(args.profile_dir or "").strip()
    if not profile_dir and persist_login:
        profile_dir = str(
            cleanup_cfg.get("profile_dir", "./config/playwright-like-cleanup-profile")
            or ""
        ).strip()
    if not persist_login:
        profile_dir = ""
    wait_timeout_seconds = int(
        args.wait_timeout_seconds
        if args.wait_timeout_seconds is not None
        else cleanup_cfg.get("wait_timeout_seconds", 600) or 600
    )
    request_interval_ms = int(
        args.request_interval_ms
        if args.request_interval_ms is not None
        else cleanup_cfg.get("request_interval_ms", 1000) or 1000
    )

    batch_label = "explicit selection" if explicit_aweme_ids else batch_scope
    print(
        f"[INFO] Found {len(aweme_ids)} downloaded aweme id(s) to unlike "
        f"(batch_scope={batch_label})."
    )

    def _on_progress(payload: Dict[str, Any]) -> None:
        event = str(payload.get("event") or "").strip()
        if event:
            aweme_id = str(payload.get("aweme_id") or "")
            suffix = f" {aweme_id}" if aweme_id else ""
            print(f"[INFO] Like cleanup state: {event}{suffix}", flush=True)
            return
        index = int(payload.get("index", 0) or 0)
        total = int(payload.get("total", len(aweme_ids)) or len(aweme_ids))
        aweme_id = str(payload.get("aweme_id") or "")
        status = str(payload.get("status") or "unknown")
        status_code = payload.get("status_code")
        status_msg = str(payload.get("status_msg") or "").strip()
        detail_parts = []
        if status_code is not None:
            detail_parts.append(f"code={status_code}")
        if status_msg:
            detail_parts.append(status_msg)
        detail_suffix = f" ({', '.join(detail_parts)})" if detail_parts else ""
        print(
            f"[INFO] Like cleanup progress: {index}/{total} {status} {aweme_id}{detail_suffix}",
            flush=True,
        )

    async def _wait_for_login_confirmation(message: str) -> None:
        print(f"[INFO] {message}", flush=True)
        await asyncio.to_thread(input)

    async with DouyinAPIClient(cookies, proxy=config.get("proxy")) as api_client:
        result = await api_client.cancel_likes_via_browser(
            aweme_ids,
            headless=headless,
            wait_timeout_seconds=wait_timeout_seconds,
            request_interval_ms=request_interval_ms,
            profile_dir=profile_dir or None,
            progress_callback=_on_progress,
            login_confirmation_callback=_wait_for_login_confirmation,
        )

    success_count = int(result.get("success_count", 0) or 0)
    failed_ids = result.get("failed_ids") or []
    print(
        f"[INFO] Like cleanup finished: success={success_count}, failed={len(failed_ids)}"
    )
    if failed_ids:
        print("[WARN] Failed aweme ids:")
        for aweme_id in failed_ids:
            print(aweme_id)
        return 2
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
