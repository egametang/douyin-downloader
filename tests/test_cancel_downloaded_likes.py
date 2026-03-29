import json

from tools.cancel_downloaded_likes import parse_args
from tools.cancel_downloaded_likes import (
    collect_aweme_ids_from_db,
    collect_aweme_ids_from_manifest,
    collect_downloaded_aweme_ids,
)


def test_collect_aweme_ids_from_db_returns_latest_unique_ids(tmp_path):
    db_path = tmp_path / "dy_downloader.db"

    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE aweme (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                aweme_id TEXT UNIQUE NOT NULL,
                download_time INTEGER
            )
            """
        )
        conn.execute(
            "INSERT INTO aweme (aweme_id, download_time) VALUES (?, ?)",
            ("111", 100),
        )
        conn.execute(
            "INSERT INTO aweme (aweme_id, download_time) VALUES (?, ?)",
            ("222", 200),
        )
        conn.commit()
    finally:
        conn.close()

    assert collect_aweme_ids_from_db(db_path) == ["222", "111"]


def test_collect_aweme_ids_from_db_latest_batch_stops_at_large_gap(tmp_path):
    db_path = tmp_path / "dy_downloader.db"

    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE aweme (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                aweme_id TEXT UNIQUE NOT NULL,
                download_time INTEGER
            )
            """
        )
        conn.execute(
            "INSERT INTO aweme (aweme_id, download_time) VALUES (?, ?)",
            ("old-1", 100),
        )
        conn.execute(
            "INSERT INTO aweme (aweme_id, download_time) VALUES (?, ?)",
            ("latest-1", 1000),
        )
        conn.execute(
            "INSERT INTO aweme (aweme_id, download_time) VALUES (?, ?)",
            ("latest-2", 995),
        )
        conn.commit()
    finally:
        conn.close()

    assert collect_aweme_ids_from_db(
        db_path,
        batch_scope="latest",
        batch_gap_seconds=120,
    ) == ["latest-1", "latest-2"]


def test_collect_aweme_ids_from_manifest_dedupes_and_skips_invalid_lines(tmp_path):
    manifest_path = tmp_path / "download_manifest.jsonl"
    manifest_path.write_text(
        "\n".join(
            [
                json.dumps({"aweme_id": "111"}, ensure_ascii=False),
                "{not-json",
                json.dumps({"aweme_id": "222"}, ensure_ascii=False),
                json.dumps({"aweme_id": "111"}, ensure_ascii=False),
            ]
        ),
        encoding="utf-8",
    )

    assert collect_aweme_ids_from_manifest(manifest_path) == ["111", "222"]


def test_collect_downloaded_aweme_ids_combines_db_and_manifest(tmp_path):
    db_path = tmp_path / "dy_downloader.db"
    manifest_path = tmp_path / "download_manifest.jsonl"

    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE aweme (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                aweme_id TEXT UNIQUE NOT NULL,
                download_time INTEGER
            )
            """
        )
        conn.execute(
            "INSERT INTO aweme (aweme_id, download_time) VALUES (?, ?)",
            ("111", 100),
        )
        conn.commit()
    finally:
        conn.close()

    manifest_path.write_text(
        json.dumps({"aweme_id": "222"}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    assert collect_downloaded_aweme_ids(
        db_path,
        manifest_path,
        "auto",
        batch_scope="all",
    ) == [
        "111",
        "222",
    ]


def test_collect_downloaded_aweme_ids_latest_auto_prefers_db_batch(tmp_path):
    db_path = tmp_path / "dy_downloader.db"
    manifest_path = tmp_path / "download_manifest.jsonl"

    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE aweme (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                aweme_id TEXT UNIQUE NOT NULL,
                download_time INTEGER
            )
            """
        )
        conn.execute(
            "INSERT INTO aweme (aweme_id, download_time) VALUES (?, ?)",
            ("old-1", 100),
        )
        conn.execute(
            "INSERT INTO aweme (aweme_id, download_time) VALUES (?, ?)",
            ("latest-1", 1000),
        )
        conn.execute(
            "INSERT INTO aweme (aweme_id, download_time) VALUES (?, ?)",
            ("latest-2", 999),
        )
        conn.commit()
    finally:
        conn.close()

    manifest_path.write_text(
        "\n".join(
            [
                json.dumps({"aweme_id": "manifest-old"}, ensure_ascii=False),
                json.dumps({"aweme_id": "manifest-older"}, ensure_ascii=False),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    assert collect_downloaded_aweme_ids(
        db_path,
        manifest_path,
        "auto",
        batch_scope="latest",
        batch_gap_seconds=120,
    ) == ["latest-1", "latest-2"]


def test_parse_args_supports_multiple_aweme_ids():
    args = parse_args(
        [
            "--aweme-id",
            "111",
            "--aweme-id",
            "222",
        ]
    )

    assert args.aweme_ids == ["111", "222"]


def test_parse_args_defaults_latest_batch_scope():
    args = parse_args([])

    assert args.batch_scope == "latest"
    assert args.batch_gap_seconds == 900
