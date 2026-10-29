"""
ci_seed_warehouse.py  (Phase 7 - CI only)
=========================================
Seeds the CI Postgres with a small, DETERMINISTIC batch of clean payment
rows so the dbt job can build the star schema and run all 20 tests on
every push - no Kafka, no Spark, no S3 in the loop.

Why deterministic? CI must be reproducible: the same commit must always
see the same data, so a red build means the CODE changed behaviour, never
the dice. Why only clean rows? This seed emulates SILVER (post-quarantine)
data - the contract dbt builds on.
"""
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
from faker import Faker

N_ROWS = 300
rng = random.Random(42)
fake = Faker()
Faker.seed(42)

MERCHANTS = [
    {
        "merchant_id": f"M{1000 + i}",
        "merchant_name": fake.company(),
        "merchant_country": rng.choice(["IE", "GB", "FR", "DE", "ES", "NL"]),
        "merchant_category": rng.choice(
            ["retail", "food_beverage", "travel", "digital_services", "hospitality"]),
    }
    for i in range(40)
]

BASE = datetime(2026, 10, 27, 9, 0, tzinfo=timezone.utc)

rows = []
for i in range(N_ROWS):
    m = rng.choice(MERCHANTS)
    t = BASE + timedelta(minutes=7 * i)
    rows.append((
        str(uuid.UUID(int=rng.getrandbits(128))),       # deterministic uuid
        t, t.date(), m["merchant_id"], m["merchant_name"],
        m["merchant_country"], m["merchant_category"],
        f"C{100000 + rng.randrange(400)}",
        round(rng.uniform(1, 500), 2),
        rng.choice(["EUR", "GBP", "USD"]),
        rng.choice(["card", "bank_transfer", "wallet", "direct_debit"]),
        rng.choice(["authorised", "settled", "declined", "refunded"]),
        0, i, t, t.date(),
    ))

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

dsn = os.getenv(
    "WAREHOUSE_DSN",
    "host=localhost port=5432 dbname=lakehouse user=lakehouse password=lakehouse",
)
with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    cur.execute(DDL)
    cur.execute("truncate table raw.silver_payments;")
    cur.executemany(
        "insert into raw.silver_payments values "
        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        rows,
    )
    cur.execute("select count(*) from raw.silver_payments;")
    print(f"seeded {cur.fetchone()[0]} deterministic rows for the dbt CI job")
