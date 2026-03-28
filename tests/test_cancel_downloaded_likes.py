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

    assert collect_downloaded_aweme_ids(db_path, manifest_path, "auto") == [
        "111",
        "222",
    ]


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
