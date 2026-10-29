"""Behavioural tests for the Great Expectations gates (no S3 involved):
build synthetic dataframes and prove each gate passes what it should
pass and FAILS what it should fail - a quality gate that can't fail is
decoration, not protection."""
import random
import uuid

import pytest

pytest.importorskip(
    "great_expectations",
    reason="GX not installed (runs in CI / the quality container)",
)
import pandas as pd  # noqa: E402
from run_quality_checks import validate  # noqa: E402

RNG = random.Random(7)


def make_frame(n, bad_rate=0.0, with_dups=False, silver=False):
    rows = []
    for _ in range(n):
        bad = RNG.random() < bad_rate
        rows.append({
            "event_id": str(uuid.uuid4()),
            "event_time": "2026-10-29T10:00:00+00:00",
            "merchant_id": None if (bad and RNG.random() < 0.33)
            else f"M{1000 + RNG.randrange(200)}",
            "customer_id": f"C{100000 + RNG.randrange(5000)}",
            "amount": -5.0 if (bad and RNG.random() < 0.5)
            else round(RNG.uniform(1, 500), 2),
            "currency": "EU" if bad else RNG.choice(["EUR", "GBP", "USD"]),
            "payment_method": "card",
            "status": "settled",
        })
    df = pd.DataFrame(rows)
    if with_dups:
        df = pd.concat([df, df.head(3)], ignore_index=True)
    if silver:
        df["event_date"] = "2026-10-29"
    return df


def test_bronze_gate_tolerates_known_noise_and_duplicates():
    df = make_frame(1500, bad_rate=0.01, with_dups=True)
    assert validate(df, "bronze") is True


def test_bronze_gate_fails_on_degraded_feed():
    df = make_frame(1500, bad_rate=0.10)
    assert validate(df, "bronze") is False


def test_silver_gate_passes_clean_data():
    df = make_frame(1500, silver=True)
    assert validate(df, "silver") is True


def test_silver_gate_fails_on_a_single_bad_row():
    df = make_frame(1500, silver=True)
    df.loc[10, "amount"] = -9.99
    assert validate(df, "silver") is False
