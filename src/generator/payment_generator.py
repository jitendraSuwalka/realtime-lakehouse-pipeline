"""
payment_generator.py
====================
Generates a continuous stream of SIMULATED payment events and sends them
to a Kafka topic — the entry point of the whole lakehouse pipeline.

  ┌───────────────┐      JSON events       ┌─────────┐
  │ this script   │ ──────────────────────▶ │  Kafka  │ ──▶ Bronze layer (later)
  └───────────────┘   topic: payments.raw   └─────────┘

Why simulated data?
  Real payment data is confidential. Simulating it is standard practice
  for portfolio projects — and lets us control volume and "messiness".

Why are ~1% of events deliberately invalid?
  Real-world feeds are messy (nulls, negative amounts, bad codes).
  We inject a small number of bad events ON PURPOSE so the data-quality
  checks downstream (Great Expectations) have something real to catch.
  Without this, "data quality" would be an empty claim.

Usage:
  python3 src/generator/payment_generator.py --dry-run --limit 5
      → prints 5 events to the screen, no Kafka needed (good first test)

  python3 src/generator/payment_generator.py
      → sends events to Kafka forever at EVENTS_PER_SEC (Ctrl+C to stop)

Configuration (environment variables, all optional):
  BOOTSTRAP_SERVERS  Kafka address        (default: localhost:9092)
  TOPIC              Kafka topic name     (default: payments.raw)
  EVENTS_PER_SEC     events per second    (default: 5)
"""

import argparse
import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from faker import Faker

# ---------------------------------------------------------------------------
# Configuration — read once at start-up, with sensible defaults
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "payments.raw")
EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "5"))
BAD_EVENT_RATE = 0.01  # ~1% of events are deliberately invalid (see docstring)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("payment-generator")

fake = Faker()
Faker.seed(42)   # same merchant/customer names on every run → reproducible
pool_rng = random.Random(42)  # dedicated seeded RNG: pool attributes stay
                              # identical across runs - event flow stays random
random.seed()    # but random event flow each run

# ---------------------------------------------------------------------------
# Fixed pools of merchants and customers.
# WHY fixed pools (not random every time)? Downstream analytics (revenue per
# merchant, customer behaviour) only make sense if the same IDs keep
# re-appearing — exactly like a real payment platform.
# ---------------------------------------------------------------------------
MERCHANTS = [
    {
        "merchant_id": f"M{1000 + i}",
        "merchant_name": fake.company(),
        "merchant_country": random.choice(["IE", "GB", "FR", "DE", "ES", "NL"]),
        "merchant_category": pool_rng.choice(
            ["retail", "food_beverage", "travel", "digital_services", "hospitality"]
        ),
    }
    for i in range(200)  # 200 merchants
]

CUSTOMER_IDS = [f"C{100000 + i}" for i in range(5000)]  # 5,000 customers

CURRENCIES = ["EUR", "GBP", "USD"]
CURRENCY_WEIGHTS = [0.6, 0.3, 0.1]          # mostly EUR — an Irish platform
PAYMENT_METHODS = ["card", "bank_transfer", "wallet", "direct_debit"]
STATUSES = ["authorised", "settled", "declined", "refunded"]
STATUS_WEIGHTS = [0.55, 0.35, 0.07, 0.03]   # most payments succeed


def build_event() -> dict:
    """Build one simulated payment event as a Python dictionary."""
    merchant = random.choice(MERCHANTS)
    event = {
        "event_id": str(uuid.uuid4()),                       # unique per event
        "event_time": datetime.now(timezone.utc).isoformat(),
        "merchant_id": merchant["merchant_id"],
        "merchant_name": merchant["merchant_name"],
        "merchant_country": merchant["merchant_country"],
        "merchant_category": merchant["merchant_category"],
        "customer_id": random.choice(CUSTOMER_IDS),
        "amount": round(random.lognormvariate(3.0, 1.0), 2),  # realistic skew:
        #   many small payments, a few large ones (median ≈ €20)
        "currency": random.choices(CURRENCIES, CURRENCY_WEIGHTS)[0],
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choices(STATUSES, STATUS_WEIGHTS)[0],
    }

    # ------------------------------------------------------------------
    # Deliberate data-quality problems in ~1% of events (see docstring).
    # Each bad event gets exactly one of three realistic defects.
    # ------------------------------------------------------------------
    if random.random() < BAD_EVENT_RATE:
        defect = random.choice(["negative_amount", "missing_merchant", "bad_currency"])
        if defect == "negative_amount":
            event["amount"] = -abs(event["amount"])
        elif defect == "missing_merchant":
            event["merchant_id"] = None
        elif defect == "bad_currency":
            event["currency"] = "EU"  # invalid ISO code

    return event


def run_dry(limit: int) -> None:
    """Print events to the screen instead of sending to Kafka."""
    for _ in range(limit):
        print(json.dumps(build_event(), indent=2))
    log.info("Dry run complete: %d events printed.", limit)


def run_kafka(limit: int | None) -> None:
    """Send events to Kafka at EVENTS_PER_SEC until Ctrl+C (or limit)."""
    # Imported here (not at the top) so --dry-run works even before
    # confluent-kafka is installed.
    from confluent_kafka import Producer

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            # Wait for the broker to confirm it has stored the message.
            "acks": "all",
        }
    )

    stop = {"flag": False}

    def on_sigint(signum, frame):
        stop["flag"] = True
        log.info("Stopping… flushing unsent events to Kafka.")

    signal.signal(signal.SIGINT, on_sigint)

    def delivery_report(err, msg):
        """Called once per message to confirm delivery (or report failure)."""
        if err is not None:
            log.error("Delivery failed: %s", err)

    sent = 0
    sleep_between = 1.0 / EVENTS_PER_SEC
    log.info(
        "Producing to topic '%s' at %s events/sec (Ctrl+C to stop)…",
        TOPIC,
        EVENTS_PER_SEC,
    )
    while not stop["flag"]:
        event = build_event()
        producer.produce(
            topic=TOPIC,
            # Keyed by merchant: all events for one merchant go to the same
            # Kafka partition, which keeps them in order per merchant.
            key=str(event["merchant_id"]).encode(),
            value=json.dumps(event).encode(),
            callback=delivery_report,
        )
        producer.poll(0)  # serve delivery callbacks
        sent += 1
        if sent % 100 == 0:
            log.info("Sent %d events…", sent)
        if limit and sent >= limit:
            break
        time.sleep(sleep_between)

    producer.flush(10)  # make sure nothing is lost on shutdown
    log.info("Done. %d events sent to '%s'.", sent, TOPIC)


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulated payment event generator")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print events to screen instead of sending to Kafka",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="stop after this many events (default: run forever)",
    )
    args = parser.parse_args()

    if args.dry_run:
        run_dry(limit=args.limit or 5)
    else:
        run_kafka(limit=args.limit)


if __name__ == "__main__":
    main()
