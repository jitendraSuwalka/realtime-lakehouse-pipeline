# Architecture

> This document grows with the build. Each phase adds its section and the
> design decisions behind it. The final diagram lands here in Phase 7.

## The medallion pattern in one paragraph

Data arrives messy and gets progressively more trustworthy as it moves
through three layers: **Bronze** stores every raw event exactly as received
(the permanent audit copy — never edited), **Silver** holds cleaned,
deduplicated, correctly-typed data, and **Gold** holds business-ready tables
(a star schema: one `fact_payments` table plus dimension tables for
merchants, customers, dates). Analysts and dashboards only ever touch Gold.

## Event schema (produced by the generator)

| Field | Type | Notes |
|---|---|---|
| event_id | uuid string | unique per event |
| event_time | ISO-8601 string (UTC) | when the payment happened |
| merchant_id | string, `M####` | 200 fixed merchants; ~1% deliberately null |
| merchant_name / country / category | strings | denormalised onto the event |
| customer_id | string, `C######` | 5,000 fixed customers |
| amount | float | log-normal: many small, few large; ~1% deliberately negative |
| currency | EUR / GBP / USD | ~1% deliberately invalid ("EU") |
| payment_method | card / bank_transfer / wallet / direct_debit | |
| status | authorised / settled / declined / refunded | weighted realistic mix |

## Phase log

### Phase 0 — scaffold (done)
- Kafka 3.7 in **KRaft mode** (no ZooKeeper), single node, dual listeners:
  `localhost:9092` for the laptop, `kafka:19092` for other containers.
- Kafka UI on `http://localhost:8080` for visibility while developing.
- Generator produces keyed-by-merchant JSON events to topic `payments.raw`.

### Phase 1 — bronze layer (done)
- **LocalStack** joins docker-compose as the local S3 (AWS API on `:4566`).
  The bronze writer talks to it through `boto3` with an `endpoint_url` —
  dropping that one argument points the same code at real AWS S3 in Phase 6.
- *Build incident, kept for honesty:* the layer was designed on **MinIO**,
  but during the build its public images became unavailable (Docker Hub:
  pull access denied; quay.io: 401). Because the store is pure
  configuration, the swap to LocalStack cost one endpoint default and a
  compose block — a real example of why S3-compatible abstractions matter.
- `scripts/init_s3.py` creates the `lakehouse` bucket idempotently
  (LocalStack community edition doesn't persist across `docker compose
  down`; day to day use `stop`/`start`, and bronze can always be re-landed
  from Kafka).
- `src/bronze/bronze_writer.py` consumes `payments.raw` as consumer group
  `bronze-writer`, batches events in memory, and flushes every 500 events or
  30 seconds as **snappy-compressed Parquet** to
  `s3://lakehouse/bronze/payments/ingest_date=YYYY-MM-DD/`.
- Each row gains **lineage columns**: `kafka_partition`, `kafka_offset`,
  `ingested_at`.
- Semantics are **at-least-once** (a crash between a file write and an
  offset commit can duplicate a few events). Deduplication on `event_id`
  is Silver's job — a standard, defensible trade-off.
- The deliberately-bad events (negative amounts, null merchant_id, bad
  currency codes) are **kept** in bronze, by design: bronze is the audit
  copy, and Great Expectations will catch them at the Silver gate.

### Phase 2 — silver layer (done)
- `src/silver/silver_job.py`, a **PySpark** batch job run inside the
  `apache/spark:3.5.1` container (`docker compose run --rm spark-job`),
  reading and writing S3 through the **S3A** connector
  (`hadoop-aws`, path-style access for the emulator).
- Why containerised: the laptop's Python 3.14 is newer than PySpark
  supports, and production Spark is containerised/clustered anyway; the
  image ships its own Python and Java. `local[2]` = Spark local mode.
- Transformations: **typing** (timestamps, double amount, derived
  `event_date`), **deduplication** on `event_id` keeping the latest
  `ingested_at` copy (window + row_number — deterministic), and
  **validation** splitting events into `silver/payments/` (partitioned by
  `event_date`, the business date) and `silver/quarantine/payments/`
  (tagged with `reject_reason`: missing_merchant, non_positive_amount,
  invalid_currency).
- **Quarantine, don't delete**: rejects stay queryable — counted in the
  job summary, inspected with `scripts/peek_silver.py`, and formalised by
  Great Expectations in Phase 5.
- **Full refresh** each run (`mode=overwrite`): simple and idempotent at
  this scale. Incremental per-partition processing arrives with Airflow
  (Phase 4).

### Phase 3 — gold layer (done)
- **Postgres 16** joins compose as the warehouse (Redshift's stand-in until
  Phase 6; dbt makes that swap a profile change, not a rewrite).
- `scripts/load_silver_to_warehouse.py` carries silver from S3 into
  `raw.silver_payments` with **TRUNCATE + COPY** (bulk protocol,
  idempotent), recovering `event_date` from each file's partition path.
- **dbt** (dbt-postgres 1.9 in an image built from `docker/dbt.Dockerfile`,
  Python 3.12 inside) builds the star schema in schema `analytics`:
  `stg_payments` (view) → `fact_payments` + `dim_merchants`,
  `dim_customers`, `dim_dates` (tables).
- **dbt tests** encode the layer contracts: `unique`/`not_null` on
  `event_id` (proves the Spark dedupe), `accepted_values` on
  currency/status (proves the quarantine gate), and `relationships` from
  every fact key to its dimension (proves the star's referential
  integrity). `docker compose run --rm dbt test` must pass before a commit.
- `scripts/peek_gold.py` runs the project's first business query: top
  merchants by settled revenue, fact joined to dimension.

### Phase 4 — orchestration (done)
- **Airflow 2.10** (image built from `docker/airflow.Dockerfile`, pinned
  with Airflow's official constraints file) runs scheduler + webserver via
  `standalone` in one container; metadata in Postgres (database `airflow`),
  LocalExecutor; UI on `http://localhost:8081`.
- One DAG, `lakehouse_pipeline` (`@daily`, `catchup=False`, 2 retries/task):
  `bronze_ingest → silver_spark → load_warehouse → dbt_run → dbt_test`.
- **Two operator patterns**: BashOperator runs bronze and the loader inside
  the Airflow container (its image carries their deps); **DockerOperator**
  launches the same `apache/spark:3.5.1` and project dbt containers used
  manually, over the mounted Docker socket, attached to the compose network
  by name. Airflow conducts; containers compute.
- `bronze_writer.py` gained `--max-idle-seconds`: the forever-consumer
  becomes a bounded batch drain (exit after N quiet seconds) — the
  difference between a streaming service and a schedulable task.
- **DockerOperator gotcha, documented**: it talks to the HOST daemon, so
  bind mounts need host paths — compose injects the repo path as
  `HOST_REPO`.
- The DAG is **idempotent end to end** (consumer-group drain, full-refresh
  silver, TRUNCATE+COPY load, dbt rebuild): any run can be repeated safely.

### Phase 5 — data-quality gates (done)
- **Great Expectations** (1.x, in an image built from
  `docker/quality.Dockerfile`, Python 3.12 inside) runs two gates inside
  the DAG: `bronze_ingest → quality_bronze → silver_spark →
  quality_silver → load_warehouse → ...`.
- **Gate A (bronze) is a tolerant feed monitor**: schema + identity fields
  are hard rules, but merchant_id nulls, invalid currencies and
  non-positive amounts use `mostly=0.95` thresholds — the deliberate ~1%
  bad events are *known noise*, and the gate trips only when the feed
  *degrades*. No uniqueness rule on bronze: at-least-once duplicates are
  allowed there by design.
- **Gate B (silver) is an absolute contract**: `event_id` unique and
  never null, every amount positive, every currency valid, `event_date`
  present — silver claims to be clean, so a single bad row fails the
  task, and Airflow stops everything downstream. Bad data never reaches
  the warehouse.
- Suites are **defined in code** (`src/quality/run_quality_checks.py`),
  version-controlled next to the pipeline they guard — the same
  philosophy as dbt's `schema.yml` tests, one layer earlier.
- Division of labour: **GX guards the lake, dbt tests guard the
  warehouse.** Gate B proves silver's cleaning worked; dbt's 20 tests
  prove the star schema built from it is internally consistent.
