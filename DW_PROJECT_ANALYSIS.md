# Data Warehouse Project Analysis

## Architecture Assessment

Overall this is a solid portfolio project with a realistic streaming DW pattern. Here are the flaws and gaps I found, organized by severity.

---

## Critical Flaws

### 1. Airflow DAG runs every 1 minute with full SQL rewrites

`warehouse_pipeline.py` triggers `INSERT INTO ... SELECT` (full table replacement?) every 60 seconds. If these are `INSERT OVERWRITE` or `TRUNCATE + INSERT`, you're reprocessing the entire dataset every minute. At scale this is expensive and fragile. ClickHouse also doesn't handle high-frequency small writes well — it merges parts in the background and frequent inserts create excessive parts.

**Fix:** Use incremental loads — track a `last_processed_at` watermark and only process new/changed records. Or change the schedule to hourly/daily for batch transforms.

---

### 2. No data quality layer between Bronze and Silver

You go straight from raw ingestion to transformation with no validation. If a malformed record lands in bronze (null `order_id`, bad timestamp format, duplicate PK), it silently propagates to Silver and Gold and corrupts your fact table.

**Fix:** Add a validation step between bronze and silver — at minimum assert NOT NULL on key fields, check referential integrity, flag duplicates.

---

### 3. KSQL-DB is deployed but apparently unused

You have `ksqldb-server` running, but the actual stream processing is done in Python consumers. This wastes resources and adds confusion about what does what.

**Fix:** Either use KSQL for stream transformations (filter, join, aggregate in-stream before hitting ClickHouse), or remove it entirely.

---

### 4. Hardcoded credentials everywhere

`admin/admin` for PostgreSQL, ClickHouse, and Airflow. Sensitive values directly in `docker-compose.yml`. Fine for local dev, but if this repo is public on GitHub, those are exposed.

**Fix:** Use a `.env` file (already in `.gitignore`) and reference variables via `${VAR}` in docker-compose. Your `.gitignore` has `M` status — verify secrets aren't tracked.

---

## Architectural Gaps

### 5. No schema registry

Redpanda supports a schema registry (port 8081 is exposed), but your producer sends raw JSON with no schema enforcement. If the producer changes a field name or type, the consumer silently breaks or corrupts data.

**Fix:** Use Avro or Protobuf schemas with the Redpanda Schema Registry. `confluent-kafka` already supports this.

---

### 6. Dimensions as ClickHouse Dictionaries — refresh strategy unclear

ClickHouse dictionaries cache data and need explicit refresh. If `dict_customers` is loaded once and never refreshed, new customers in the Gold layer won't resolve correctly.

**Fix:** Set `LIFETIME(MIN 300 MAX 360)` on dictionaries and ensure they're backed by ClickHouse tables (not PostgreSQL directly), which seems to be what you're doing — just verify the refresh is actually happening.

---

### 7. ML is disconnected from the pipeline

The ML scripts in `ml/` appear to be run manually. They're not wired into the Airflow DAG, so there's no scheduled retraining. Forecasts and segments get stale.

**Fix:** Add ML tasks to the Airflow DAG (e.g., weekly retraining downstream of Gold transforms).

---

### 8. No dead-letter queue visibility for ClickHouse consumer

`consumer_postgres.py` has a dead-letter retry mechanism. `consumer_clickhouse.py` does not — failed inserts are dropped silently.

**Fix:** Mirror the retry + dead-letter pattern from the Postgres consumer into the ClickHouse consumer.

---

## What's Missing (Technologies Worth Adding)

| Gap | Recommendation | Why |
|-----|---------------|-----|
| **Data quality** | Great Expectations or **dbt tests** | Validate Bronze→Silver transitions |
| **Transformation layer** | **dbt** | Replace raw SQL files with versioned, testable, documented models. Huge for a DW project |
| **Data catalog** | Apache Atlas or **OpenMetadata** | Lineage tracking from Kafka → Bronze → Gold is invisible right now |
| **Observability** | **Prometheus + Grafana** | Monitor consumer lag, ClickHouse query times, Airflow task duration |
| **Kafka consumer lag** | **Redpanda Console** already exists | Just set up lag alerts — it's already deployed |

**dbt is the most impactful addition.** Your Silver/Gold SQL (`transform_bronze_to_silver.sql`, `transform_silver_to_gold.sql`) would map directly to dbt models. You'd get: incremental models, built-in tests, auto-generated docs, and lineage graphs — all things that demonstrate production-readiness to employers.

---

## What's Done Well

- Medallion architecture (Bronze/Silver/Gold) is correctly implemented
- Star schema in Gold with proper fact + dimension separation
- Redpanda over vanilla Kafka is a good choice (lighter, faster for dev)
- FK-safe insertion ordering in the Postgres consumer is thoughtful
- Offset commit only after successful insert = at-least-once delivery guarantee
- Producer state file for resume capability
- ClickHouse dictionaries for dimension lookups instead of JOINs — correct pattern
- ML outputs written back to ClickHouse (not just flat files) is the right approach

---

## Priority Order

1. **Add dbt** — biggest architectural improvement, most portfolio value
2. **Fix incremental loads** in Airflow (critical correctness issue)
3. **Add data quality checks** between Bronze and Silver
4. **Wire ML into Airflow DAG** for scheduled retraining
5. **Add Prometheus/Grafana** for observability
6. **Remove KSQL or actually use it**
