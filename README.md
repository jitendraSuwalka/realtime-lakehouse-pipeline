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
 │ (payment  │    │topic: │    │ raw      │    │ cleaned, │    │ dbt star      │
 │  events)  │    │payments│   │ Parquet  │    │ deduped  │    │ schema in     │
 └───────────┘    │ .raw  │    │ on S3    │    │ (Spark)  │    │ Redshift      │
                  └───────┘    │ (Local-  │    └──────────┘    └───────────────┘
                               │  Stack)  │
                               └──────────┘
        Airflow orchestrates ── Great Expectations guards quality
        Terraform provisions AWS ── GitHub Actions runs CI
```

(Full diagram: [docs/architecture.md](docs/architecture.md))

## Build status

- [x] **Phase 0** — project scaffold, Kafka (KRaft) via Docker Compose, payment event generator
- [x] **Phase 1** — Bronze layer: Kafka consumer landing raw events as date-partitioned Parquet in local S3 (LocalStack)
- [x] **Phase 2** — Silver layer: Spark (containerised) cleaning, typing, dedupe on event_id, quarantine of bad events
- [x] **Phase 3** — Gold layer: dbt star schema in Postgres (fact_payments + merchant/customer/date dims) with dbt tests
- [x] **Phase 4** — Airflow DAG orchestrating bronze → silver → warehouse → dbt build + test (BashOperator + DockerOperator, retries, daily schedule)
- [x] **Phase 5** — Great Expectations quality gates: tolerant bronze feed monitor + strict silver contract, enforced in the DAG
- [ ] **Phase 6** — AWS: Terraform-provisioned S3 + Redshift Serverless, end-to-end cloud run
- [ ] **Phase 7** — CI/CD with GitHub Actions; final docs and demo

## Quickstart (what works so far)

```bash
# 1. start Kafka, Kafka UI and LocalStack (local S3)
docker compose up -d

# 2. install Python dependencies
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. test the generator without Kafka
python3 src/generator/payment_generator.py --dry-run --limit 5

# 4. stream events into Kafka (Ctrl+C to stop)
python3 src/generator/payment_generator.py
#    watch them arrive: http://localhost:8080  (Kafka UI → topic payments.raw)

# 5. create the bucket (idempotent - run any time), then land events in bronze
python3 scripts/init_s3.py
python3 src/bronze/bronze_writer.py --limit 1000

# 6. verify what landed
python3 scripts/peek_bronze.py

# 7. build the silver layer: clean, type, dedupe, quarantine (Spark in Docker;
#    first run downloads the Spark image + S3 connector jars - be patient)
docker compose run --rm spark-job
python3 scripts/peek_silver.py

# 7b. data-quality gates (Great Expectations; one-off: docker compose build quality)
docker compose run --rm quality --layer bronze    # tolerant feed monitor
docker compose run --rm quality --layer silver    # strict contract

# 8. gold layer: load silver into the warehouse, build + test the star schema
python3 scripts/load_silver_to_warehouse.py
docker compose run --rm dbt run
docker compose run --rm dbt test
python3 scripts/peek_gold.py

# 9. or let Airflow run steps 5-8 (quality gates included) as one DAG (UI: http://localhost:8081;
#    one-off: create its metadata DB, then build + start it)
docker compose exec postgres psql -U lakehouse -c "CREATE DATABASE airflow;"
docker compose build airflow && docker compose up -d airflow
docker compose exec airflow cat standalone_admin_password.txt   # login: admin
#    then unpause `lakehouse_pipeline` in the UI and press ▶ Trigger
```

## Design decisions

Recorded as the build progresses — see [docs/architecture.md](docs/architecture.md).

| Decision | Why |
|---|---|
| Kafka in **KRaft mode** | Modern Kafka without ZooKeeper — one service fewer to run |
| **~1% deliberately invalid events** | Gives downstream data-quality checks something real to catch |
| **Fixed merchant/customer pools** | Repeating IDs make per-merchant analytics meaningful, like a real platform |
| **LocalStack locally, S3 in cloud** | Same S3 API — identical pipeline code in both environments. (Started on MinIO; swapped mid-build when its public images vanished from Docker Hub and quay.io — one config value changed, zero code. LocalStack then pinned to 3.8 when `latest` began requiring a licence token.) |
| **Bronze stores events unmodified** (bad ones included) | Bronze is the permanent audit copy; cleaning belongs to Silver |
| **At-least-once ingestion, dedupe in Silver** | Simpler and safer than pretending exactly-once; Silver dedupes on `event_id` |
| **Hive-style partitioning** (`ingest_date=` in bronze, `event_date=` in silver) | Bronze partitions by arrival, silver by business date — analysts think in business dates |
| **Quarantine, don't delete** | Rejects land in `silver/quarantine/` with a `reject_reason` — countable, inspectable, alertable |
| **Containerised Spark job** (`docker compose run`) | The container brings its own Python/Java (laptop Python 3.14 is ahead of PySpark); mirrors how production Spark runs |
| **Postgres as the dev warehouse** | Same dbt models later target Redshift via a profile change — the migration is config, not SQL |
| **dbt tests on every layer contract** | unique/not_null/accepted_values/relationships prove silver's guarantees and the star schema's referential integrity on every run |
| **Two-speed quality gates** | Bronze gate uses GX `mostly` thresholds (the known ~1% bad events are noise, not news); silver gate is absolute — silver claims to be clean, so one bad row stops the DAG before the warehouse |
| **Airflow as conductor, not compute** | Tasks launch the same Spark/dbt containers used manually (DockerOperator over the socket); heavy work stays out of the scheduler |
| **Idempotent DAG** | Consumer-group drain + full-refresh layers + TRUNCATE/COPY mean any run can be safely repeated — the property that makes schedules operable |

## Licence

MIT — see [LICENSE](LICENSE).
