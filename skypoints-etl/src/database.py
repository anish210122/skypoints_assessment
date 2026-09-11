"""SQLite database loading functions."""
import json
import sqlite3
from pathlib import Path
import pandas as pd

COUNTRY_TABLES = {"USA": "table_usa", "IND": "table_ind", "AUS": "table_aus"}


def create_schema(conn, ddl_path: Path):
    conn.executescript(ddl_path.read_text(encoding="utf-8-sig"))


def insert_bronze_profiles(conn, records, payload_hash):
    sql = """INSERT OR IGNORE INTO raw_member_landing
             (source_hash, record_type, member_name, member_id, enrollment_date,
              flight_date, tier_code, agent_name, state, country, post_code, dob, is_active)
             VALUES (?, 'PROFILE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    rows = [
        (
            payload_hash(r), r.get("member_name"), r.get("member_id"),
            r.get("enrollment_date"), r.get("flight_date"), r.get("tier_code"),
            r.get("agent_name"), r.get("state"), r.get("country"),
            r.get("post_code"), r.get("dob"), r.get("is_active", "A"),
        )
        for r in records
    ]
    conn.executemany(sql, rows)


def insert_bronze_redemptions(conn, entries, payload_hash):
    sql = """INSERT OR IGNORE INTO raw_redemption_landing
             (source_hash, member_id, feed_date, payload)
             VALUES (?, ?, ?, ?)"""
    rows = [
        (
            payload_hash(entry),
            entry.get("member_id"),
            entry.get("feed_date"),
            json.dumps(entry, sort_keys=True),
        )
        for entry in entries
    ]
    conn.executemany(sql, rows)


def load_staging(conn, records):
    conn.execute("DELETE FROM stg_member_profiles")
    columns = [
        "member_id", "member_name", "enrollment_date", "flight_date", "tier_code",
        "agent_name", "state", "country", "post_code", "dob", "age",
        "is_stale_member", "is_active",
    ]
    pd.DataFrame(records, columns=columns).to_sql(
        "stg_member_profiles", conn, if_exists="append", index=False
    )


def load_country_targets(conn, records):
    columns = [
        "member_id", "member_name", "enrollment_date", "flight_date", "tier_code",
        "agent_name", "state", "country", "post_code", "dob", "age",
        "is_stale_member", "is_active",
    ]
    df = pd.DataFrame(records, columns=columns)
    for country, table in COUNTRY_TABLES.items():
        conn.execute(f"DELETE FROM {table}")
        df[df["country"] == country].to_sql(
            table, conn, if_exists="append", index=False
        )


def upsert_redemptions(conn, records):
    sql = """INSERT OR IGNORE INTO fct_member_redemptions
             (txn_id, member_id, feed_date, txn_date, partner, miles_redeemed, status)
             VALUES (?, ?, ?, ?, ?, ?, ?)"""
    rows = [
        (
            r.get("txn_id"), r.get("member_id"), r.get("feed_date"),
            r.get("txn_date"), r.get("partner"), r.get("miles_redeemed"), r.get("status")
        )
        for r in records
    ]
    conn.executemany(sql, rows)


def table_count(conn, table_name):
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
