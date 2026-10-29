"""Unit tests for the event generator - the pipeline's front door."""
import json
import subprocess
import sys
from pathlib import Path

import payment_generator as gen

ROOT = Path(__file__).resolve().parent.parent


def test_merchant_pool_is_deterministic_across_runs():
    """REGRESSION TEST for the Phase-4 incident: merchant attributes once
    came from the unseeded global RNG, so every generator session re-rolled
    country/category and dbt's unique test on dim_merchants failed with 176
    duplicates. This test rebuilds the pool in two FRESH interpreters and
    demands they match exactly - if anyone reintroduces the bug, CI goes red
    before the data ever could."""
    snippet = (
        "import sys; sys.path.insert(0, 'src/generator'); "
        "import json, payment_generator as g; "
        "print(json.dumps(g.MERCHANTS, sort_keys=True))"
    )
    runs = [
        subprocess.run(
            [sys.executable, "-c", snippet],
            cwd=ROOT, capture_output=True, text=True, check=True,
        ).stdout
        for _ in range(2)
    ]
    assert runs[0] == runs[1], "merchant pool differs between fresh runs"
    assert len(json.loads(runs[0])) == 200


def test_event_schema_and_domains():
    """Every event carries the full contract; values stay in the domains
    the dbt accepted_values tests encode (bad events may add 'EU')."""
    expected_keys = {
        "event_id", "event_time", "merchant_id", "merchant_name",
        "merchant_country", "merchant_category", "customer_id",
        "amount", "currency", "payment_method", "status",
    }
    events = [gen.build_event() for _ in range(2000)]
    for e in events:
        assert set(e) == expected_keys
        assert e["currency"] in {"EUR", "GBP", "USD", "EU"}
        assert e["status"] in {"authorised", "settled", "declined", "refunded"}
        assert e["payment_method"] in {"card", "bank_transfer", "wallet", "direct_debit"}


def test_bad_event_rate_is_about_one_percent():
    """The deliberate messiness stays a trickle, not a flood: with
    rate=0.01 over 5,000 events, the bad count lands in a generous band."""
    events = [gen.build_event() for _ in range(5000)]
    bad = [
        e for e in events
        if e["merchant_id"] is None or e["amount"] <= 0 or e["currency"] == "EU"
    ]
    assert 10 <= len(bad) <= 120, f"bad-event count out of band: {len(bad)}"
