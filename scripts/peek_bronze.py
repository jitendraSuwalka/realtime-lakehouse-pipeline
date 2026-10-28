"""
peek_bronze.py — a small verification tool: lists what is in the bronze
layer and shows the first rows of the newest Parquet file.

Usage:  python3 scripts/peek_bronze.py
"""

import io
import os

import boto3
import pyarrow.parquet as pq

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:4566")
BUCKET = os.getenv("BUCKET", "lakehouse")
PREFIX = "bronze/payments/"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
)

resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
objects = resp.get("Contents", [])

if not objects:
    print(f"No files under s3://{BUCKET}/{PREFIX} yet - run the bronze writer first.")
    raise SystemExit(1)

print(f"{len(objects)} file(s) in the bronze layer:\n")
total_bytes = 0
for o in sorted(objects, key=lambda x: x["Key"]):
    print(f"  {o['Key']}   {o['Size'] / 1024:.1f} KB")
    total_bytes += o["Size"]
print(f"\nTotal: {total_bytes / 1024:.1f} KB")

newest = max(objects, key=lambda x: x["LastModified"])
print(f"\n--- newest file: {newest['Key']} ---")
body = s3.get_object(Bucket=BUCKET, Key=newest["Key"])["Body"].read()
table = pq.read_table(io.BytesIO(body))
print(f"rows: {table.num_rows}, columns: {table.num_columns}")
print("columns:", table.column_names)
print("\nfirst 5 rows:")
print(table.to_pandas().head(5).to_string())
