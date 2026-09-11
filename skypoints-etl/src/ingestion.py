"""Source ingestion and JSON flattening."""
import hashlib
import json


def payload_hash(payload):
    """Create a deterministic hash for idempotent Bronze ingestion."""
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def load_profile_source(path):
    with open(path, "r", encoding="utf-8-sig") as file:
        data = json.load(file)
    if not isinstance(data, list):
        raise ValueError("Profile source must contain a JSON array.")
    return data


def load_redemption_source(path):
    with open(path, "r", encoding="utf-8-sig") as file:
        data = json.load(file)
    if not isinstance(data, list):
        raise ValueError("Redemption source must contain a JSON array.")
    return data


def flatten_redemptions(entries, parse_date):
    """Flatten nested redemption arrays into one row per transaction."""
    flattened = []
    for entry in entries:
        member_id = entry.get("member_id")
        feed_date = parse_date(entry.get("feed_date"))
        for txn in entry.get("redemptions") or []:
            flattened.append(
                {
                    "txn_id": txn.get("txn_id"),
                    "member_id": member_id,
                    "feed_date": feed_date,
                    "txn_date": parse_date(txn.get("txn_date")),
                    "partner": txn.get("partner"),
                    "miles_redeemed": txn.get("miles_redeemed"),
                    "status": txn.get("status"),
                }
            )
    return flattened
