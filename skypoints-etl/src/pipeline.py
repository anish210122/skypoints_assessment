"""SkyPoints end-to-end ETL pipeline.

Flow:
Source -> Bronze -> Silver -> Gold.
The batch is deterministic for a supplied REFERENCE_DATE and idempotent
for repeated execution of the same source records.
"""
import argparse
import logging
import os
import sqlite3
from datetime import date
from pathlib import Path

from .database import (
    create_schema,
    insert_bronze_profiles,
    insert_bronze_redemptions,
    load_staging,
    load_country_targets,
    upsert_redemptions,
    table_count,
)
from .ingestion import load_profile_source, load_redemption_source, flatten_redemptions, payload_hash
from .transformations import normalize_profile, latest_member_records, parse_flexible_date

LOGGER = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB = BASE_DIR / "skypoints.db"
DEFAULT_REFERENCE_DATE = date(2024, 3, 1)


def validate_records(records):
    """Validate business-critical Silver records before database loading."""
    errors = []
    for record in records:
        if not record.get("member_id"):
            errors.append("member_id is missing")
        if not record.get("member_name"):
            errors.append(f"member_name is missing for {record.get('member_id')}")
        if not record.get("enrollment_date"):
            errors.append(f"invalid enrollment_date for {record.get('member_id')}")
        if record.get("country") not in {"USA", "IND", "AUS"}:
            errors.append(f"invalid country for {record.get('member_id')}: {record.get('country')}")
    if errors:
        raise ValueError("Silver data-quality validation failed: " + "; ".join(errors))


def run_pipeline(
    db_path=DEFAULT_DB,
    profile_path=None,
    redemption_path=None,
    reference_date=DEFAULT_REFERENCE_DATE,
):
    profile_path = Path(profile_path or BASE_DIR / "data" / "member_profiles.json")
    redemption_path = Path(redemption_path or BASE_DIR / "data" / "redemptions.json")
    db_path = Path(db_path)

    LOGGER.info("Starting SkyPoints ETL")
    LOGGER.info("Reference date: %s", reference_date)

    raw_profiles = load_profile_source(profile_path)
    raw_redemptions = load_redemption_source(redemption_path)
    LOGGER.info("Read %d profile records and %d redemption payloads", len(raw_profiles), len(raw_redemptions))

    normalized = [normalize_profile(r, reference_date) for r in raw_profiles]
    latest_records = latest_member_records(normalized)
    validate_records(latest_records)

    flattened = flatten_redemptions(raw_redemptions, parse_flexible_date)
    LOGGER.info("Flattened %d redemption transactions", len(flattened))

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        create_schema(conn, BASE_DIR / "sql" / "01_ddl_tables.sql")

        # Bronze is append-only with deterministic hashes, so rerunning the same
        # source batch does not create duplicate raw records.
        insert_bronze_profiles(conn, raw_profiles, payload_hash)
        insert_bronze_redemptions(conn, raw_redemptions, payload_hash)

        # Silver/Gold are current-state outputs for this assessment batch.
        load_staging(conn, latest_records)
        load_country_targets(conn, latest_records)

        # Fact loading is idempotent by txn_id primary key.
        upsert_redemptions(conn, flattened)

        conn.commit()
        LOGGER.info(
            "ETL completed: %d current members, USA=%d, IND=%d, AUS=%d, redemptions=%d",
            table_count(conn, "stg_member_profiles"),
            table_count(conn, "table_usa"),
            table_count(conn, "table_ind"),
            table_count(conn, "table_aus"),
            table_count(conn, "fct_member_redemptions"),
        )
    except Exception:
        conn.rollback()
        LOGGER.exception("ETL failed; transaction rolled back")
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Run the SkyPoints ETL pipeline.")
    parser.add_argument("--db", default=os.getenv("SKYPOINTS_DB", str(DEFAULT_DB)))
    parser.add_argument("--reference-date", default=os.getenv("REFERENCE_DATE", DEFAULT_REFERENCE_DATE.isoformat()))
    parser.add_argument("--profile-file", default=None)
    parser.add_argument("--redemption-file", default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    run_pipeline(
        db_path=args.db,
        profile_path=args.profile_file,
        redemption_path=args.redemption_file,
        reference_date=date.fromisoformat(args.reference_date),
    )


if __name__ == "__main__":
    main()
