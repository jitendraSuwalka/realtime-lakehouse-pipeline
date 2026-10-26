# Real-Time Lakehouse Pipeline — Medallion Architecture on AWS

An end-to-end streaming data platform for simulated payment events:
**Kafka → Bronze (raw) → Silver (Spark-cleaned) → Gold (dbt star schema in Redshift)**,
orchestrated with **Airflow**, quality-guarded with **Great Expectations**,
provisioned with **Terraform**, and tested in CI with **GitHub Actions**.

> ⚠️ **All data is simulated.** Payment events are produced by
> [`src/generator/payment_generator.py`](src/generator/payment_generator.py) —
> no real payment information exists anywhere in this project.

## Target architecture

```
 ┌───────────┐    ┌───────┐    ┌──────────┐    ┌──────────┐    ┌───────────────┐
 │ Generator │───▶│ Kafka │───▶│  BRONZE  │───▶│  SILVER  │───▶│     GOLD      │
 │ (payment  │    │topic: │    │ raw JSON │    │ cleaned, │    │ dbt star      │
 │  events)  │    │payments│   │ on S3    │    │ deduped  │    │ schema in     │
 └───────────┘    │ .raw  │    │ (MinIO   │    │ (Spark)  │    │ Redshift      │
                  └───────┘    │  local)  │    └──────────┘    └───────────────┘
                               └──────────┘
        Airflow orchestrates ── Great Expectations guards quality
        Terraform provisions AWS ── GitHub Actions runs CI
```

(Full diagram: [docs/architecture.md](docs/architecture.md))

## Build status

- [x] **Phase 0** — project scaffold, Kafka (KRaft) via Docker Compose, payment event generator
- [ ] **Phase 1** — Bronze layer: Kafka consumer landing raw events as partitioned Parquet in MinIO/S3
- [ ] **Phase 2** — Silver layer: Spark cleaning, deduplication, typing
- [ ] **Phase 3** — Gold layer: dbt star schema (fact_payments + dimensions)
- [ ] **Phase 4** — Airflow orchestration of the batch hops
- [ ] **Phase 5** — Great Expectations validation suites at each layer
- [ ] **Phase 6** — AWS: Terraform-provisioned S3 + Redshift Serverless, end-to-end cloud run
- [ ] **Phase 7** — CI/CD with GitHub Actions; final docs and demo

## Quickstart (what works so far)

```bash
# 1. start Kafka + Kafka UI
docker compose up -d

# 2. install Python dependencies
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. test the generator without Kafka
python3 src/generator/payment_generator.py --dry-run --limit 5

# 4. stream events into Kafka (Ctrl+C to stop)
python3 src/generator/payment_generator.py

# 5. watch them arrive: http://localhost:8080  (Kafka UI → topic payments.raw)
```

## Design decisions

Recorded as the build progresses — see [docs/architecture.md](docs/architecture.md).

| Decision | Why |
|---|---|
| Kafka in **KRaft mode** | Modern Kafka without ZooKeeper — one service fewer to run |
| **~1% deliberately invalid events** | Gives downstream data-quality checks something real to catch |
| **Fixed merchant/customer pools** | Repeating IDs make per-merchant analytics meaningful, like a real platform |
| **MinIO locally, S3 in cloud** | Same S3 API — the pipeline code is identical in both environments |

## Licence

MIT — see [LICENSE](LICENSE).
