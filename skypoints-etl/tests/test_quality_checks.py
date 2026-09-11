from datetime import date
import json
import sqlite3

from src.ingestion import flatten_redemptions
from src.pipeline import run_pipeline
from src.transformations import (
    compute_age,
    compute_stale_flag,
    latest_member_records,
    parse_flexible_date,
)


def test_parse_flexible_date():
    assert parse_flexible_date("20240115") == "2024-01-15"
    assert parse_flexible_date(6152022) == "2022-06-15"
    assert parse_flexible_date("2021-13-13") is None
    assert parse_flexible_date(None) is None


def test_compute_age():
    ref = date(2024, 1, 1)
    assert compute_age("1990-01-01", ref_date=ref) == 34
    assert compute_age("1990-06-01", ref_date=ref) == 33
    assert compute_age(None, ref_date=ref) is None


def test_compute_stale_flag():
    ref = date(2024, 4, 1)
    assert compute_stale_flag("2024-02-01", ref_date=ref) == 0
    assert compute_stale_flag("2023-11-01", ref_date=ref) == 1
    assert compute_stale_flag(None, ref_date=ref) == 1


def test_stale_boundary_is_not_stale_at_90_days():
    ref = date(2024, 4, 1)
    assert compute_stale_flag("2024-01-02", ref_date=ref) == 0


def test_latest_record_wins():
    records = [
        {"member_id": "1", "flight_date": "2024-01-01", "enrollment_date": "2020-01-01"},
        {"member_id": "1", "flight_date": "2024-02-01", "enrollment_date": "2020-01-01"},
        {"member_id": "2", "flight_date": "2024-02-01", "enrollment_date": "2020-01-01"},
    ]
    latest = latest_member_records(records)
    by_id = {r["member_id"]: r for r in latest}
    assert len(latest) == 2
    assert by_id["1"]["flight_date"] == "2024-02-01"


def test_flatten_redemptions_handles_missing_array():
    entries = [{"member_id": "1", "feed_date": "20240115", "redemptions": None}]
    assert flatten_redemptions(entries, parse_flexible_date) == []


def test_pipeline_end_to_end(tmp_path):
    db = tmp_path / "test.db"
    run_pipeline(db_path=db, reference_date=date(2024, 3, 1))

    with sqlite3.connect(db) as conn:
        # Latest-record-wins: Ravi is in USA, not India.
        assert conn.execute("SELECT COUNT(*) FROM table_usa WHERE member_id='223458'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM table_ind WHERE member_id='223458'").fetchone()[0] == 0

        # Current members are Elena, Ravi and Jacob.
        assert conn.execute("SELECT COUNT(*) FROM stg_member_profiles").fetchone()[0] == 3

        # JSON contains two flattened transactions.
        assert conn.execute("SELECT COUNT(*) FROM fct_member_redemptions").fetchone()[0] == 2

        # Bronze stores source records and redemption payloads.
        assert conn.execute("SELECT COUNT(*) FROM raw_member_landing").fetchone()[0] == 4
        assert conn.execute("SELECT COUNT(*) FROM raw_redemption_landing").fetchone()[0] == 1


def test_pipeline_is_idempotent_for_same_source_batch(tmp_path):
    db = tmp_path / "idempotent.db"
    run_pipeline(db_path=db, reference_date=date(2024, 3, 1))
    run_pipeline(db_path=db, reference_date=date(2024, 3, 1))

    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM raw_member_landing").fetchone()[0] == 4
        assert conn.execute("SELECT COUNT(*) FROM raw_redemption_landing").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stg_member_profiles").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM fct_member_redemptions").fetchone()[0] == 2

        # Verify the DDL primary key survived loading.
        columns = conn.execute("PRAGMA table_info(stg_member_profiles)").fetchall()
        member_id = next(c for c in columns if c[1] == "member_id")
        assert member_id[5] == 1
