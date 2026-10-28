"""
init_s3.py — create the lakehouse bucket if it doesn't exist (idempotent:
safe to run any number of times).

Run this once after starting the services:
    python3 scripts/init_s3.py

Why a script instead of clicking in a console? Infrastructure you can re-run
is infrastructure you can't forget how to set up — the same principle
Terraform applies to the real AWS environment in Phase 6.
"""

import os

import boto3
from botocore.exceptions import ClientError

ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:4566")
BUCKET = os.getenv("BUCKET", "lakehouse")

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
)

try:
    s3.head_bucket(Bucket=BUCKET)
    print(f"bucket '{BUCKET}' already exists at {ENDPOINT} - nothing to do")
except ClientError:
    s3.create_bucket(Bucket=BUCKET)
    print(f"bucket '{BUCKET}' created at {ENDPOINT}")
