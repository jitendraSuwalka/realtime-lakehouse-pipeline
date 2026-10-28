"""
load_silver_to_warehouse.py
===========================
Carries the SILVER layer (clean Parquet in S3) into the WAREHOUSE
(Postgres locally; Redshift in Phase 6), where dbt then builds the
gold star schema.

  ┌──────────────┐    read     ┌──────────────┐   COPY      ┌──────────────┐
  │ SILVER       │ ──────────▶ │ this script  │ ──────────▶ │ Postgres     │
  │ Parquet, S3  │             │ (boto3 +     │             │ raw.silver_  │
  │              │             │  pyarrow)    │             │ payments     │
  └──────────────┘             └──────────────┘             └──────────────┘

Notes worth saying in an interview:
  - event_date is NOT inside the Parquet files (it's the partition folder
    name), so this loader recovers it from each file's PATH — the
    "partition columns live in paths, not files" lesson, applied.
  - The load is TRUNCATE + COPY: a full refresh, idempotent, matching the
    silver job. COPY is Postgres' bulk-load protocol — far faster than
    row-by-row INSERTs.
  - In Phase 6 this hop is replaced by Redshift COPY straight from S3;
    the shape of the step (S3 -> warehouse staging table) is identical.

Usage:  python3 scripts/load_silver_to_warehouse.py
"""

import io
import os
import re

import boto3
import pyarrow.parquet as pq
import psycopg

S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:4566")
BUCKET = os.getenv("BUCKET", "lakehouse")
SILVER_PREFIX = "silver/payments/"

PG_DSN = os.getenv(
    "WAREHOUSE_DSN",
    "host=localhost port=5432 dbname=lakehouse user=lakehouse password=lakehouse",
)

DDL = """
create schema if not exists raw;
create table if not exists raw.silver_payments (
    event_id          text,
    event_time        timestamptz,
    event_date        date,
    merchant_id       text,
    merchant_name     text,
    merchant_country  text,
    merchant_category text,
    customer_id       text,
    amount            double precision,
    currency          text,
    payment_method    text,
    status            text,
    kafka_partition   int,
    kafka_offset      bigint,
    ingested_at       timestamptz,
    ingest_date       date
);
"""

COLUMNS = [
    "event_id", "event_time", "event_date", "merchant_id", "merchant_name",
    "merchant_country", "merchant_category", "customer_id", "amount",
    "currency", "payment_method", "status", "kafka_partition",
    "kafka_offset", "ingested_at", "ingest_date",
]

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
)

# --- collect the silver files ------------------------------------------------
resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=SILVER_PREFIX)
files = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
if not files:
    print("No silver data found - run: docker compose run --rm spark-job")
    raise SystemExit(1)

rows = []
for key in files:
    # recover event_date from the Hive-partitioned PATH (not in the file)
    m = re.search(r"event_date=(\d{4}-\d{2}-\d{2})", key)
    event_date = m.group(1) if m else None

    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    for rec in table.to_pylist():
        rec["event_date"] = event_date
        rows.append(tuple(rec.get(c) for c in COLUMNS))

print(f"read {len(rows)} silver rows from {len(files)} Parquet file(s)")

# --- load into Postgres with COPY (bulk protocol) ----------------------------
with psycopg.connect(PG_DSN) as conn:
    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("truncate table raw.silver_payments;")
        with cur.copy(
            f"copy raw.silver_payments ({', '.join(COLUMNS)}) from stdin"
        ) as copy:
            for row in rows:
                copy.write_row(row)
        cur.execute("select count(*) from raw.silver_payments;")
        n = cur.fetchone()[0]
    conn.commit()

print(f"loaded {n} rows into raw.silver_payments (truncate + COPY, idempotent)")
