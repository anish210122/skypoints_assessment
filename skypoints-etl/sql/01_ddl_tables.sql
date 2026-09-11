-- SkyPoints ETL database schema
PRAGMA foreign_keys = ON;

-- Bronze: immutable-ish raw profile records from the source feed.
CREATE TABLE IF NOT EXISTS raw_member_landing (
    raw_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_hash     TEXT NOT NULL UNIQUE,
    record_type     TEXT NOT NULL DEFAULT 'PROFILE',
    member_name     TEXT,
    member_id       TEXT,
    enrollment_date TEXT,
    flight_date     TEXT,
    tier_code       TEXT,
    agent_name      TEXT,
    state           TEXT,
    country         TEXT,
    post_code       TEXT,
    dob             TEXT,
    is_active       TEXT,
    loaded_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Bronze: raw partner redemption payloads before flattening.
CREATE TABLE IF NOT EXISTS raw_redemption_landing (
    raw_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_hash     TEXT NOT NULL UNIQUE,
    member_id       TEXT,
    feed_date       TEXT,
    payload         TEXT NOT NULL,
    loaded_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Silver: one current record per member after normalization and deduplication.
CREATE TABLE IF NOT EXISTS stg_member_profiles (
    member_id        TEXT PRIMARY KEY,
    member_name      TEXT NOT NULL,
    enrollment_date  DATE NOT NULL,
    flight_date      DATE,
    tier_code        TEXT,
    agent_name       TEXT,
    state            TEXT,
    country          TEXT NOT NULL CHECK (country IN ('USA', 'IND', 'AUS')),
    post_code        TEXT,
    dob              DATE,
    age              INTEGER CHECK (age IS NULL OR age BETWEEN 0 AND 115),
    is_stale_member  INTEGER NOT NULL CHECK (is_stale_member IN (0, 1)),
    is_active        TEXT
);

-- Gold: country-specific outputs required by the assessment.
CREATE TABLE IF NOT EXISTS table_usa (
    member_id TEXT PRIMARY KEY, member_name TEXT NOT NULL, enrollment_date DATE NOT NULL,
    flight_date DATE, tier_code TEXT, agent_name TEXT, state TEXT, country TEXT NOT NULL DEFAULT 'USA',
    post_code TEXT, dob DATE, age INTEGER, is_stale_member INTEGER, is_active TEXT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS table_ind (
    member_id TEXT PRIMARY KEY, member_name TEXT NOT NULL, enrollment_date DATE NOT NULL,
    flight_date DATE, tier_code TEXT, agent_name TEXT, state TEXT, country TEXT NOT NULL DEFAULT 'IND',
    post_code TEXT, dob DATE, age INTEGER, is_stale_member INTEGER, is_active TEXT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS table_aus (
    member_id TEXT PRIMARY KEY, member_name TEXT NOT NULL, enrollment_date DATE NOT NULL,
    flight_date DATE, tier_code TEXT, agent_name TEXT, state TEXT, country TEXT NOT NULL DEFAULT 'AUS',
    post_code TEXT, dob DATE, age INTEGER, is_stale_member INTEGER, is_active TEXT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Gold: flattened partner redemptions.
CREATE TABLE IF NOT EXISTS fct_member_redemptions (
    txn_id TEXT PRIMARY KEY,
    member_id TEXT NOT NULL,
    feed_date DATE,
    txn_date DATE,
    partner TEXT,
    miles_redeemed INTEGER CHECK (miles_redeemed IS NULL OR miles_redeemed >= 0),
    status TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_member_id ON raw_member_landing(member_id);
CREATE INDEX IF NOT EXISTS idx_stg_country ON stg_member_profiles(country);
CREATE INDEX IF NOT EXISTS idx_redemptions_member_id ON fct_member_redemptions(member_id);
CREATE INDEX IF NOT EXISTS idx_redemptions_txn_date ON fct_member_redemptions(txn_date);
