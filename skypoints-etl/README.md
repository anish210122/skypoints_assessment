# SkyPoints Data Platform – Incubyte Assessment

A reproducible **ETL pipeline** implementing a Medallion-style architecture for the SkyPoints loyalty program.

## Architecture

```text
Profile JSON / partner JSON
          |
          v
   +-------------+
   | Bronze      |  raw_member_landing
   | raw payload |  raw_redemption_landing
   +------+------+
          |
          v
   +-------------+
   | Silver      |  normalized dates, age,
   | current     |  stale flag, latest record wins
   +------+------+
          |
       +--+--+--+
       v  v  v
      USA IND AUS        Gold country targets
          |
          v
   Redemption fact       Gold flattened transactions
```

### Bronze
- Preserves source profile records.
- Preserves partner redemption payloads as JSON.
- Uses deterministic source hashes to make repeated ingestion of the same source record idempotent.

### Silver
- Normalizes supported date formats to `YYYY-MM-DD`.
- Converts invalid calendar dates to NULL.
- Derives birthday-aware customer age.
- Calculates `is_stale_member` using a configurable reference date.
- Applies **Latest Record Wins** using flight date, enrollment date, and source order as deterministic tie-breakers.

### Gold
- Routes each current member to exactly one country table: `table_usa`, `table_ind`, or `table_aus`.
- Flattens nested redemption arrays into `fct_member_redemptions`.
- Uses `txn_id` as the fact-table primary key and ignores duplicate transaction ingestion.

## Business rules demonstrated

1. **Corrupt dates:** values such as `2021-13-13` become NULL.
2. **Unpadded numeric dates:** `6152022` becomes `2022-06-15`.
3. **Latest Record Wins:** Ravi's later USA record supersedes his older IND record.
4. **Stale member:** missing flight date or a flight more than 90 days before the reference date is stale.
5. **Age:** calculated using the customer's birthday, not only birth year.
6. **Nested JSON:** every redemption transaction becomes one fact row.

## Project structure

```text
skypoints-etl/
├── data/
│   ├── member_profiles.json
│   └── redemptions.json
├── sql/
│   ├── 01_ddl_tables.sql
│   ├── 02_analytics_and_validations.sql
│   └── 03_data_quality.sql
├── src/
│   ├── __init__.py
│   ├── database.py
│   ├── ingestion.py
│   ├── pipeline.py
│   └── transformations.py
├── tests/
│   └── test_quality_checks.py
├── requirements.txt
└── README.md
```

## Quickstart

From the `skypoints-etl` directory:

```powershell
python -m venv venv
.env\Scripts\Activate.ps1
pip install -r requirements.txt
python -m src.pipeline
python -m pytest -q
```

The default assessment reference date is `2024-03-01`.

You can override it:

```powershell
python -m src.pipeline --reference-date 2024-03-31
```

You can also override the database location:

```powershell
python -m src.pipeline --db .\output\skypoints.db
```

or with environment variables:

```powershell
$env:REFERENCE_DATE="2024-03-31"
$env:SKYPOINTS_DB="output\skypoints.db"
python -m src.pipeline
```

## Expected assessment output

With the supplied sample data:

- Current members: **3**
- USA members: **2** (Elena, Ravi)
- IND members: **0**
- AUS members: **1** (Jacob)
- Redemption transactions: **2**
- Bronze profile records: **4**
- Bronze redemption payloads: **1**

Ravi demonstrates the relocation rule: his older IND record is superseded by his newer USA record.

## Validation

Run the analytics and validation SQL after the pipeline:

```powershell
sqlite3 skypoints.db < sql/02_analytics_and_validations.sql
sqlite3 skypoints.db < sql/03_data_quality.sql
```

If SQLite CLI is not installed, the queries can be opened in any SQLite client.

## Testing approach

The test suite covers:

- valid and invalid date parsing
- unpadded numeric dates
- birthday-aware age calculation
- stale-member boundary behaviour
- latest-record-wins deduplication
- JSON flattening with missing redemption arrays
- end-to-end country routing
- end-to-end redemption loading
- Bronze ingestion
- idempotent reruns
- preservation of database primary keys

## Design decisions

### Why SQLite?
SQLite keeps the assessment self-contained and reproducible without requiring a database server. The same ETL concepts can be moved to PostgreSQL, Azure SQL, Snowflake or another production platform.

### Why ETL rather than ELT?
The source is extracted, normalized/validated in Python, and then loaded into relational target tables. Therefore this implementation is more accurately described as **ETL**.

### Why separate country tables?
The assessment requires country-specific target tables. In a production model, a canonical member dimension with a `country` attribute plus country-specific views could reduce schema duplication.

### Idempotency
Bronze records use a deterministic source hash. Silver/current-state tables are rebuilt for the batch, while redemption facts use `txn_id` and `INSERT OR IGNORE` to prevent duplicate transactions.

## Production extensions

For a larger production implementation, I would add:

- orchestration with Airflow/Azure Data Factory
- cloud object storage for Bronze
- Spark/PySpark for large datasets
- structured logging and monitoring
- data-quality/quarantine tables
- secrets/configuration management
- CI/CD with automated tests
- incremental watermarks and batch IDs
