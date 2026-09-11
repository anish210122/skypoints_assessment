"""Pure transformation functions for the SkyPoints ETL pipeline."""
from datetime import date, datetime
import pandas as pd

SUPPORTED_DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d", "%m%d%Y")


def parse_flexible_date(value):
    """Normalize supported source date formats to ISO YYYY-MM-DD; invalid values -> None."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    value_str = str(value).strip()
    if not value_str or value_str.upper() == "NAT":
        return None
    value_str = value_str.split()[0]

    if value_str.isdigit() and len(value_str) in (7, 8):
        value_str = value_str.zfill(8)

    for fmt in SUPPORTED_DATE_FORMATS:
        try:
            return datetime.strptime(value_str, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def compute_age(dob_str, ref_date=None):
    """Calculate birthday-aware integer age from an ISO date."""
    if not dob_str:
        return None
    ref_date = ref_date or date.today()
    try:
        born = date.fromisoformat(dob_str)
        return ref_date.year - born.year - ((ref_date.month, ref_date.day) < (born.month, born.day))
    except (TypeError, ValueError):
        return None


def compute_stale_flag(flight_date_str, ref_date=None):
    """Return 1 when flight is missing or more than 90 days old; otherwise 0."""
    if not flight_date_str:
        return 1
    ref_date = ref_date or date.today()
    try:
        flight_dt = date.fromisoformat(flight_date_str)
        diff_days = (ref_date - flight_dt).days
        return int(diff_days > 90)
    except (TypeError, ValueError):
        return 1


def normalize_profile(record, ref_date):
    """Convert one raw profile record into a normalized Silver-layer record."""
    enrollment_date = parse_flexible_date(record.get("enrollment_date"))
    flight_date = parse_flexible_date(record.get("flight_date"))
    dob = parse_flexible_date(record.get("dob"))
    country = (record.get("country") or "").strip().upper()

    return {
        "member_id": str(record.get("member_id")).strip() if record.get("member_id") is not None else None,
        "member_name": record.get("member_name"),
        "enrollment_date": enrollment_date,
        "flight_date": flight_date,
        "tier_code": record.get("tier_code"),
        "agent_name": record.get("agent_name"),
        "state": record.get("state"),
        "country": country,
        "post_code": record.get("post_code"),
        "dob": dob,
        "age": compute_age(dob, ref_date),
        "is_stale_member": compute_stale_flag(flight_date, ref_date),
        "is_active": record.get("is_active", "A"),
    }


def latest_member_records(records):
    """Return one deterministic latest record per member."""
    if not records:
        return []

    df = pd.DataFrame(records)
    df["_flight_sort"] = pd.to_datetime(df["flight_date"], errors="coerce")
    df["_enrollment_sort"] = pd.to_datetime(df["enrollment_date"], errors="coerce")
    df["_source_order"] = range(len(df))
    df.sort_values(
        ["member_id", "_flight_sort", "_enrollment_sort", "_source_order"],
        ascending=[True, False, False, False],
        na_position="last",
        inplace=True,
    )
    df = df.drop_duplicates("member_id", keep="first")
    return df.drop(columns=["_flight_sort", "_enrollment_sort", "_source_order"]).to_dict("records")
