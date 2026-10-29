"""
run_quality_checks.py
=====================
PHASE 5 - the data-quality gates, built on Great Expectations (GX).

Where dbt tests guard the WAREHOUSE (gold), these gates guard the LAKE:

   bronze ──[ GATE A: tolerant ]──▶ Spark ──▶ silver ──[ GATE B: strict ]──▶ warehouse

  GATE A (bronze): "is the raw feed healthy?"
      Bronze deliberately contains ~1% bad events (that is the audit copy
      doing its job), so Gate A uses GX's `mostly` thresholds: e.g. "at
      least 95% of merchant_ids must be present". It fails only when the
      feed DEGRADES - a schema change, or bad rates far above normal.
      This is quality MONITORING: tolerate known noise, alarm on drift.

  GATE B (silver): "did cleaning actually clean?"
      Silver claims to be deduplicated and validated, so Gate B is
      absolute: event_id unique and never null, every amount positive,
      every currency valid. One bad row = the gate fails = the pipeline
      STOPS before bad data reaches the warehouse and the dashboards.
      This is a quality CONTRACT: zero tolerance.

Run it (inside the quality container - see docker/quality.Dockerfile):
  docker compose run --rm quality --layer bronze
  docker compose run --rm quality --layer silver

Exit code 0 = gate passed, 1 = gate failed. Airflow treats a non-zero
exit as task failure, which is exactly how the gate stops the DAG.

Configuration (environment variables, all optional):
  S3_ENDPOINT     S3 API address  (default: http://localhost:4566;
                                   inside compose: http://localstack:4566)
  S3_ACCESS_KEY / S3_SECRET_KEY   (default: minioadmin / minioadmin)
  BUCKET          bucket name     (default: lakehouse)
"""

import argparse
import io
import os
import re
import sys

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:4566")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("BUCKET", "lakehouse")

# Where each layer lives in the lake.
LAYER_PREFIX = {
    "bronze": "bronze/payments/",
    "silver": "silver/payments/",
}


# ---------------------------------------------------------------------------
# Loading: read every Parquet file under a prefix into one DataFrame.
# (Same pattern as the warehouse loader. At laptop scale this is fine;
#  in production you validate just the new partition, not all history.)
# ---------------------------------------------------------------------------
def load_layer(layer: str) -> pd.DataFrame:
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )
    prefix = LAYER_PREFIX[layer]
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    if not keys:
        print(f"ERROR: no Parquet files found under s3://{BUCKET}/{prefix}")
        sys.exit(1)

    frames = []
    for key in keys:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        part = pd.read_parquet(io.BytesIO(body))
        # Hive-style partitioning stores the partition column in the PATH,
        # not inside the file (silver/payments/event_date=2026-10-29/...).
        # Recover it from the key - exactly as the warehouse loader does.
        m = re.search(r"event_date=(\d{4}-\d{2}-\d{2})", key)
        if m is not None:
            part["event_date"] = m.group(1)
        frames.append(part)
    df = pd.concat(frames, ignore_index=True)
    print(f"Loaded {len(df):,} rows from {len(keys)} file(s) under s3://{BUCKET}/{prefix}\n")
    return df


# ---------------------------------------------------------------------------
# The expectation suites - the heart of this file.
# Each suite is the layer's quality rules written as executable statements.
# ---------------------------------------------------------------------------
def build_suite(layer: str):
    import great_expectations as gx

    E = gx.expectations
    suite = gx.ExpectationSuite(name=f"{layer}_payments")

    # --- rules shared by both layers: the schema must be intact ------------
    for col in ["event_id", "event_time", "merchant_id", "customer_id",
                "amount", "currency", "payment_method", "status"]:
        suite.add_expectation(E.ExpectColumnToExist(column=col))

    # Identity fields the generator ALWAYS sets - null here means something
    # is structurally broken, so no tolerance even in bronze.
    suite.add_expectation(E.ExpectColumnValuesToNotBeNull(column="event_id"))
    suite.add_expectation(E.ExpectColumnValuesToNotBeNull(column="event_time"))

    if layer == "bronze":
        # --- GATE A: tolerant thresholds (`mostly`) -------------------------
        # ~1% of events are deliberately bad; 95% thresholds leave headroom.
        # If a threshold trips, the FEED has changed - investigate upstream.
        suite.add_expectation(
            E.ExpectColumnValuesToNotBeNull(column="merchant_id", mostly=0.95))
        suite.add_expectation(
            E.ExpectColumnValuesToBeInSet(
                column="currency", value_set=["EUR", "GBP", "USD"], mostly=0.95))
        suite.add_expectation(
            E.ExpectColumnValuesToBeBetween(
                column="amount", min_value=0, strict_min=True, mostly=0.95))
        # Bronze is at-least-once: duplicates are ALLOWED here (silver's job).
        # So, deliberately, NO uniqueness expectation on bronze.

    else:  # silver
        # --- GATE B: the contract - absolute, no `mostly` -------------------
        suite.add_expectation(E.ExpectColumnValuesToBeUnique(column="event_id"))
        suite.add_expectation(E.ExpectColumnValuesToNotBeNull(column="merchant_id"))
        suite.add_expectation(
            E.ExpectColumnValuesToBeInSet(
                column="currency", value_set=["EUR", "GBP", "USD"]))
        suite.add_expectation(
            E.ExpectColumnValuesToBeBetween(
                column="amount", min_value=0, strict_min=True))
        # Silver adds the business-date partition column - it must be there
        # and populated, or partitioned reads downstream silently miss data.
        suite.add_expectation(E.ExpectColumnToExist(column="event_date"))
        suite.add_expectation(E.ExpectColumnValuesToNotBeNull(column="event_date"))

    return suite


# ---------------------------------------------------------------------------
# Validation: run the suite against the DataFrame and report like a human.
# ---------------------------------------------------------------------------
def validate(df: pd.DataFrame, layer: str) -> bool:
    import great_expectations as gx
    from great_expectations.data_context.types.base import (
        DataContextConfig, InMemoryStoreBackendDefaults, ProgressBarsConfig)

    # Ephemeral context = in-memory, no project folder to maintain. The
    # suites live in this file, in version control, next to the code they
    # guard - the same philosophy as dbt's schema.yml tests.
    # (progress bars off: this runs inside Airflow, where they are noise)
    context = gx.get_context(
        project_config=DataContextConfig(
            store_backend_defaults=InMemoryStoreBackendDefaults(),
            progress_bars=ProgressBarsConfig(globally=False),
        ),
        mode="ephemeral",
    )
    batch = (
        context.data_sources.add_pandas(name="lake")
        .add_dataframe_asset(name="payments")
        .add_batch_definition_whole_dataframe("batch")
        .get_batch(batch_parameters={"dataframe": df})
    )

    suite = build_suite(layer)
    results = batch.validate(suite)

    print(f"=== {layer.upper()} GATE: {len(results.results)} expectations ===")
    for r in results.results:
        cfg = r.expectation_config
        name = cfg.type.replace("expect_", "")
        col = cfg.kwargs.get("column", "")
        mostly = cfg.kwargs.get("mostly")
        tol = f" (mostly={mostly:.0%})" if mostly else ""
        mark = "PASS" if r.success else "FAIL"
        unexpected = r.result.get("unexpected_count")
        detail = ""
        if unexpected is not None:
            pct = r.result.get("unexpected_percent") or 0.0
            detail = f"  bad: {unexpected}/{r.result.get('element_count')} ({pct:.2f}%)"
        print(f"  [{mark}] {name:<28} {col:<14}{tol}{detail}")

    verdict = "PASS" if results.success else "FAIL"
    print(f"\n{layer.upper()} GATE: {verdict}")
    return bool(results.success)


def main() -> None:
    parser = argparse.ArgumentParser(description="Great Expectations quality gates")
    parser.add_argument("--layer", choices=["bronze", "silver"], required=True,
                        help="which gate to run")
    args = parser.parse_args()

    df = load_layer(args.layer)
    ok = validate(df, args.layer)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
