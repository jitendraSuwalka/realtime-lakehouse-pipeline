"""
peek_gold.py — verification tool for the gold layer: row counts of the
star schema, plus the project's first real business query.

Usage:  python3 scripts/peek_gold.py
"""

import os

import psycopg

PG_DSN = os.getenv(
    "WAREHOUSE_DSN",
    "host=localhost port=5432 dbname=lakehouse user=lakehouse password=lakehouse",
)

with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
    print("GOLD star schema (schema: analytics)\n")
    for table in ["fact_payments", "dim_merchants", "dim_customers", "dim_dates"]:
        cur.execute(f"select count(*) from analytics.{table};")
        print(f"  {table:15} {cur.fetchone()[0]:>6} rows")

    print("\nTop 5 merchants by settled revenue (fact joined to dim):\n")
    cur.execute(
        """
        select d.merchant_name,
               d.merchant_country,
               count(*)                as payments,
               round(sum(f.amount)::numeric, 2) as revenue
        from analytics.fact_payments f
        join analytics.dim_merchants d using (merchant_id)
        where f.status in ('settled', 'authorised')
        group by d.merchant_name, d.merchant_country
        order by revenue desc
        limit 5;
        """
    )
    print(f"  {'merchant':30} {'country':8} {'payments':>8} {'revenue':>10}")
    for name, country, n, rev in cur.fetchall():
        print(f"  {name:30} {country:8} {n:>8} {rev:>10}")

print("\nThat query - fact joined to dimension, grouped, ranked - is what")
print("the whole pipeline exists to make fast and trustworthy.")
