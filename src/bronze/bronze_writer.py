"""
bronze_writer.py
================
The BRONZE layer of the lakehouse: a Kafka consumer that reads payment
events from the topic `payments.raw` and lands them, unmodified, as
date-partitioned Parquet files in MinIO (locally) or S3 (in the cloud).

  ┌─────────┐   consume    ┌──────────────┐   Parquet files    ┌──────────────┐
  │  Kafka  │ ───────────▶ │ this script  │ ─────────────────▶ │ MinIO / S3   │
  │payments │              │ (batches in  │  bronze/payments/  │ bucket:      │
  │  .raw   │              │  memory)     │  ingest_date=.../  │ lakehouse    │
  └─────────┘              └──────────────┘                    └──────────────┘

Bronze-layer principles (the interview answer, in comments):
  1. Store events EXACTLY as received — including the deliberately bad ones
     (negative amounts, missing merchant_id). Bronze is the permanent audit
     copy; cleaning is Silver's job, quality checks are Great Expectations'.
  2. Add LINEAGE metadata so every row can be traced: which Kafka partition
     and offset it came from, and when we ingested it.
  3. Delivery is AT-LEAST-ONCE: if this script crashes between writing a
     file and committing offsets, some events can be written twice.
     That is a deliberate trade-off — the Silver layer deduplicates on
     event_id, which is simpler and safer than trying to be exactly-once here.

Partitioning: files land under  bronze/payments/ingest_date=YYYY-MM-DD/
  The `key=value` folder style is called Hive partitioning — Spark, dbt,
  Athena and Redshift Spectrum all understand it natively, so later layers
  can read "just yesterday" without scanning everything.

Usage:
  python3 src/bronze/bronze_writer.py              # run until Ctrl+C
  python3 src/bronze/bronze_writer.py --limit 1000 # stop after 1,000 events

Configuration (environment variables, all optional):
  BOOTSTRAP_SERVERS   Kafka address          (default: localhost:9092)
  TOPIC               topic to consume       (default: payments.raw)
  GROUP_ID            Kafka consumer group   (default: bronze-writer)
  MINIO_ENDPOINT      S3 API address         (default: http://localhost:4566)
  MINIO_ACCESS_KEY / MINIO_SECRET_KEY        (default: minioadmin / minioadmin)
  BUCKET              target bucket          (default: lakehouse)
  FLUSH_MAX_EVENTS    write a file every N events   (default: 500)
  FLUSH_SECONDS       ... or every T seconds        (default: 30)
"""

import argparse
import io
import json
import logging
import os
import signal
import time
import uuid
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "payments.raw")
GROUP_ID = os.getenv("GROUP_ID", "bronze-writer")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:4566")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("BUCKET", "lakehouse")

FLUSH_MAX_EVENTS = int(os.getenv("FLUSH_MAX_EVENTS", "500"))
FLUSH_SECONDS = float(os.getenv("FLUSH_SECONDS", "30"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bronze-writer")


def events_to_parquet_bytes(events: list[dict]) -> bytes:
    """
    Convert a list of event dictionaries into one Parquet file, in memory.

    Why Parquet (and not JSON files)? Parquet is columnar and compressed:
    it is typically 5-10x smaller than JSON and lets Spark/dbt read only
    the columns a query needs. It is THE standard lake file format.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pylist(events)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


def object_key(now: datetime) -> str:
    """
    Build the destination path (the 'object key') for one Parquet file, e.g.
      bronze/payments/ingest_date=2026-10-28/events_142530_a1b2c3d4.parquet
    The uuid suffix guarantees two flushes in the same second never collide.
    """
    return (
        f"bronze/payments/ingest_date={now:%Y-%m-%d}/"
        f"events_{now:%H%M%S}_{uuid.uuid4().hex[:8]}.parquet"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze layer: Kafka -> Parquet in MinIO/S3")
    parser.add_argument(
        "--limit", type=int, default=None,
        help="stop after this many events (default: run until Ctrl+C)",
    )
    args = parser.parse_args()

    # Imported here so the pure helpers above are testable without Kafka/MinIO.
    import boto3
    from confluent_kafka import Consumer, KafkaError

    # --- the S3 client, pointed at MinIO ------------------------------------
    # The ONLY difference between local MinIO and real AWS S3 is this
    # endpoint_url. In Phase 6 we drop it and the same code talks to AWS.
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # --- the Kafka consumer --------------------------------------------------
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            # A consumer GROUP remembers how far it has read (its 'offset').
            # Restart the script and it carries on where it left off.
            "group.id": GROUP_ID,
            # First ever run of this group: start from the oldest message,
            # so nothing already in the topic is missed.
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    batch: list[dict] = []
    total = 0
    last_flush = time.monotonic()
    stop = {"flag": False}

    def on_sigint(signum, frame):
        stop["flag"] = True
        log.info("Stopping... will flush %d buffered events first.", len(batch))

    signal.signal(signal.SIGINT, on_sigint)

    def flush() -> None:
        """Write the current batch to MinIO as one Parquet file."""
        nonlocal batch, last_flush
        if not batch:
            last_flush = time.monotonic()
            return
        now = datetime.now(timezone.utc)
        key = object_key(now)
        body = events_to_parquet_bytes(batch)
        s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        log.info("Wrote %d events (%.1f KB) -> s3://%s/%s", len(batch), len(body) / 1024, BUCKET, key)
        batch = []
        last_flush = time.monotonic()

    log.info(
        "Consuming '%s' as group '%s'; flushing every %d events or %.0fs. Ctrl+C to stop.",
        TOPIC, GROUP_ID, FLUSH_MAX_EVENTS, FLUSH_SECONDS,
    )

    try:
        while not stop["flag"]:
            msg = consumer.poll(1.0)  # wait up to 1s for the next message
            if msg is None:
                # No new message right now - a quiet topic is normal.
                if time.monotonic() - last_flush >= FLUSH_SECONDS:
                    flush()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # reached the end of what's in the topic - fine
                log.error("Kafka error: %s", msg.error())
                continue

            event = json.loads(msg.value())
            # --- lineage metadata (bronze principle #2) ---------------------
            event["kafka_partition"] = msg.partition()
            event["kafka_offset"] = msg.offset()
            event["ingested_at"] = datetime.now(timezone.utc).isoformat()
            batch.append(event)
            total += 1

            if len(batch) >= FLUSH_MAX_EVENTS or time.monotonic() - last_flush >= FLUSH_SECONDS:
                flush()
            if args.limit and total >= args.limit:
                break
    finally:
        flush()  # never lose the tail of the batch
        consumer.close()
        log.info("Done. %d events landed in bronze.", total)


if __name__ == "__main__":
    main()
