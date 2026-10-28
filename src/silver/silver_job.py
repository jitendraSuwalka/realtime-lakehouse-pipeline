"""
silver_job.py
=============
The SILVER layer: a PySpark batch job that reads everything in bronze,
cleans it, deduplicates it, and splits it into:

  - silver/payments/            -> clean, typed, deduplicated events,
                                   partitioned by event_date (business date)
  - silver/quarantine/payments/ -> the rejects, each tagged with a
                                   reject_reason column

  ┌──────────────┐    read     ┌──────────────┐    write    ┌──────────────┐
  │ BRONZE       │ ──────────▶ │  this job    │ ──────────▶ │ SILVER       │
  │ raw Parquet  │             │ (PySpark,    │       └───▶ │ + QUARANTINE │
  │ (as arrived) │             │  local mode) │             │   Parquet    │
  └──────────────┘             └──────────────┘             └──────────────┘

Silver principles (the interview answers, in comments):
  1. TYPE EVERYTHING. Bronze stores strings-as-received; silver casts
     event_time/ingested_at to real timestamps and amount to a double,
     and derives event_date — the BUSINESS date used for partitioning.
     (Bronze partitions by ingest_date = when WE received it; silver by
     event_date = when the payment HAPPENED. Analysts think in business
     dates, so silver speaks their language.)
  2. DEDUPLICATE on event_id. Bronze is at-least-once, so duplicates are
     expected and legal there. We keep the copy with the latest
     ingested_at — a deterministic rule, not "whichever came first".
  3. QUARANTINE, don't delete. Bad events are routed to a quarantine
     path with a reject_reason, so they can be counted, inspected and
     alerted on. Silently dropping data is how pipelines lose trust.
  4. FULL REFRESH for now: each run rebuilds silver from all of bronze
     (mode="overwrite"). Simple and idempotent. Incremental, per-partition
     processing arrives with Airflow in Phase 4.

Run it (from the repo root):
  docker compose run --rm spark-job
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Configuration: inside the compose network the S3 endpoint is the SERVICE
# NAME (http://localstack:4566), not localhost. In Phase 6 these settings
# disappear and s3a:// talks to real AWS.
# ---------------------------------------------------------------------------
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localstack:4566")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("BUCKET", "lakehouse")

BRONZE_PATH = f"s3a://{BUCKET}/bronze/payments/"
SILVER_PATH = f"s3a://{BUCKET}/silver/payments/"
QUARANTINE_PATH = f"s3a://{BUCKET}/silver/quarantine/payments/"

VALID_CURRENCIES = ["EUR", "GBP", "USD"]


def build_spark() -> SparkSession:
    """One SparkSession, configured to speak to our S3 (LocalStack)."""
    return (
        SparkSession.builder.appName("silver-payments")
        # --- the S3A connector settings ------------------------------------
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        # path-style URLs (http://host/bucket/key) - required by emulators
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")  # keep the console readable

    # 1. READ all of bronze --------------------------------------------------
    raw = spark.read.parquet(BRONZE_PATH)
    n_raw = raw.count()

    # 2. TYPE the columns (silver principle #1) ------------------------------
    typed = (
        raw.withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("event_date", F.to_date("event_time"))
    )

    # 3. DEDUPLICATE on event_id (silver principle #2) -----------------------
    # Rank each copy of an event_id by ingested_at (newest first) and keep
    # rank 1. Deterministic: the same input always keeps the same copy.
    w = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())
    deduped = (
        typed.withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    n_deduped = deduped.count()

    # 4. VALIDATE and split (silver principle #3) ----------------------------
    # The first failing rule names the reject_reason; NULL reason = valid.
    reject_reason = (
        F.when(F.col("merchant_id").isNull(), F.lit("missing_merchant"))
        .when(F.col("amount") <= 0, F.lit("non_positive_amount"))
        .when(~F.col("currency").isin(VALID_CURRENCIES), F.lit("invalid_currency"))
    )
    flagged = deduped.withColumn("reject_reason", reject_reason)

    valid = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = flagged.filter(F.col("reject_reason").isNotNull())
    n_valid = valid.count()
    n_rejected = rejected.count()

    # 5. WRITE both outputs (silver principle #4: full refresh) --------------
    valid.write.mode("overwrite").partitionBy("event_date").parquet(SILVER_PATH)
    rejected.write.mode("overwrite").parquet(QUARANTINE_PATH)

    # 6. SUMMARY - the numbers an operator (and Airflow, later) checks -------
    print("=" * 60)
    print("SILVER SUMMARY")
    print(f"  bronze rows read        : {n_raw}")
    print(f"  after dedupe on event_id: {n_deduped}  "
          f"({n_raw - n_deduped} duplicate(s) removed)")
    print(f"  valid -> silver         : {n_valid}")
    print(f"  rejected -> quarantine  : {n_rejected}")
    if n_rejected:
        print("  quarantine by reason:")
        for row in rejected.groupBy("reject_reason").count().collect():
            print(f"    {row['reject_reason']}: {row['count']}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
