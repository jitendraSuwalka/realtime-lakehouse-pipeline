"""Unit tests for the bronze writer's pure helpers (no Kafka, no S3)."""
import io
import re
from datetime import datetime, timezone

import bronze_writer as bw
import pyarrow.parquet as pq


def test_events_to_parquet_bytes_round_trip():
    """What goes into a bronze file must come back out identical -
    bronze's whole promise is 'stored exactly as received'."""
    events = [
        {"event_id": "a", "amount": 12.5, "currency": "EUR",
         "kafka_partition": 0, "kafka_offset": 7, "ingested_at": "t1"},
        {"event_id": "b", "amount": -3.0, "currency": "EU",   # bad stays bad
         "kafka_partition": 1, "kafka_offset": 9, "ingested_at": "t2"},
    ]
    body = bw.events_to_parquet_bytes(events)
    table = pq.read_table(io.BytesIO(body))
    assert table.num_rows == 2
    assert table.to_pylist() == events


def test_object_key_shape_and_uniqueness():
    """Keys are Hive-partitioned by ingest date and never collide."""
    now = datetime(2026, 10, 29, 14, 25, 30, tzinfo=timezone.utc)
    key = bw.object_key(now)
    assert re.fullmatch(
        r"bronze/payments/ingest_date=2026-10-29/events_142530_[0-9a-f]{8}\.parquet",
        key,
    )
    assert bw.object_key(now) != bw.object_key(now)  # uuid suffix
