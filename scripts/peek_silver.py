"""
peek_silver.py — verification tool for the silver layer: shows what landed
in silver/ and quarantine/, including the reject reasons.

Usage:  python3 scripts/peek_silver.py     (from the repo root, on your laptop)
"""

import io
import os

import boto3
import pyarrow.parquet as pq

ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:4566")
BUCKET = os.getenv("BUCKET", "lakehouse")

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
)


def parquet_objects(prefix: str):
    """All real Parquet files under a prefix (Spark also writes _SUCCESS markers)."""
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return [o for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]


def read_all(objects):
    """Read a list of Parquet objects into one pyarrow table."""
    import pyarrow as pa

    tables = []
    for o in objects:
        body = s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read()
        tables.append(pq.read_table(io.BytesIO(body)))
    return pa.concat_tables(tables) if tables else None


# --- SILVER ----------------------------------------------------------------
silver = parquet_objects("silver/payments/")
if not silver:
    print("No silver data yet - run: docker compose run --rm spark-job")
    raise SystemExit(1)

partitions = sorted({o["Key"].split("/")[2] for o in silver if "event_date=" in o["Key"]})
t = read_all(silver)
print(f"SILVER: {t.num_rows} rows in {len(silver)} file(s), "
      f"{len(partitions)} event_date partition(s)")
for p in partitions:
    print(f"  {p}")
print(f"columns ({t.num_columns}):", t.column_names)
print("\nfirst 3 rows:")
print(t.to_pandas().head(3).to_string())

# --- QUARANTINE --------------------------------------------------------------
quarantine = parquet_objects("silver/quarantine/payments/")
tq = read_all(quarantine)
if tq is None or tq.num_rows == 0:
    print("\nQUARANTINE: empty (unusual - the generator makes ~1% bad events)")
else:
    print(f"\nQUARANTINE: {tq.num_rows} rows")
    counts = tq.to_pandas()["reject_reason"].value_counts()
    for reason, n in counts.items():
        print(f"  {reason}: {n}")
    print("\nsample reject:")
    cols = ["event_id", "merchant_id", "amount", "currency", "reject_reason"]
    print(tq.to_pandas()[cols].head(3).to_string())
