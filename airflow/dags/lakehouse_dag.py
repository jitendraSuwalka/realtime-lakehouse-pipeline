"""
lakehouse_dag.py
================
ONE Airflow DAG that runs the whole medallion pipeline, in order, with
retries - plus (Phase 5) two Great Expectations quality GATES:

  bronze_ingest ─▶ quality_bronze ─▶ silver_spark ─▶ quality_silver ─▶
  (Kafka→S3)       (GATE A:          (Spark clean/    (GATE B: strict
                    tolerant feed     quarantine)      contract - one bad
                    monitor, ~1%                       row stops the DAG)
                    noise allowed)
                       ─▶ load_warehouse ─▶ dbt_run ─▶ dbt_test
                          (S3→Postgres)      (star      (20 data
                                              schema)    tests)

  A failing gate FAILS its task, and everything downstream never runs:
  bad data is stopped at the door instead of reaching the dashboards.

How each task runs - two patterns, both industry-standard:

  * BashOperator  - bronze_ingest and load_warehouse run as plain Python
    INSIDE the Airflow container (its image carries their dependencies).
  * DockerOperator - silver_spark, dbt_run and dbt_test each launch the
    SAME containers you've been running by hand (apache/spark, our dbt
    image), via the Docker socket mounted into Airflow. Airflow is a
    conductor, not a compute engine: heavy work stays in its own
    containers.

Gotcha worth remembering (it bites everyone once): DockerOperator talks to
the HOST's Docker daemon, so its volume mounts need HOST paths, not
Airflow-container paths. The compose file passes the repo's host path in
as HOST_REPO for exactly this reason.

The pipeline is IDEMPOTENT end to end: re-run it with no new events and
bronze ingests nothing new (consumer group), silver rebuilds the same
output (full refresh), the loader TRUNCATE+COPYs the same rows, and dbt
rebuilds the same tables. Safe to re-run any time - that is the property
that makes scheduled pipelines operable.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# The repo's path ON THE HOST (see gotcha above) - injected by compose.
HOST_REPO = os.environ.get("HOST_REPO", "")

# The compose network all our services share (pinned by `name:` in
# docker-compose.yml). Containers find each other by service name on it.
NETWORK = "realtime-lakehouse-pipeline_default"

SPARK_IMAGE = "apache/spark:3.5.1"
DBT_IMAGE = "realtime-lakehouse-pipeline-dbt:latest"
QUALITY_IMAGE = "realtime-lakehouse-pipeline-quality:latest"

DOCKER_COMMON = dict(
    api_version="auto",
    docker_url="unix://var/run/docker.sock",
    network_mode=NETWORK,
    auto_remove="success",   # clean up the container when the task succeeds
    mount_tmp_dir=False,     # we manage our own mounts
)

default_args = {
    "owner": "jay",
    "retries": 2,                          # every task gets two retries...
    "retry_delay": timedelta(minutes=1),   # ...a minute apart
}

with DAG(
    dag_id="lakehouse_pipeline",
    description="bronze -> silver -> warehouse -> dbt build + test",
    schedule="@daily",         # once a day; trigger manually any time
    start_date=datetime(2026, 10, 1),
    catchup=False,             # don't back-fill runs for past days
    default_args=default_args,
    tags=["lakehouse"],
) as dag:

    # 1. Drain whatever is waiting in Kafka into bronze. --max-idle-seconds
    #    makes the writer exit once the topic has been quiet for 20s -
    #    a batch drain, instead of running forever.
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=(
            "BOOTSTRAP_SERVERS=kafka:19092 "
            "MINIO_ENDPOINT=http://localstack:4566 "
            "python /opt/airflow/repo/src/bronze/bronze_writer.py "
            "--max-idle-seconds 20"
        ),
    )

    # Great Expectations gates (Phase 5) - same image, different --layer.
    # Gate A after bronze (tolerant: `mostly` thresholds absorb the known
    # ~1% bad events; it trips only if the FEED degrades). Gate B after
    # silver (strict: silver claims to be clean, so one bad row fails it).
    def quality_gate(layer: str) -> DockerOperator:
        return DockerOperator(
            task_id=f"quality_{layer}",
            image=QUALITY_IMAGE,
            command=f"--layer {layer}",   # appended to the image ENTRYPOINT
            mounts=[
                Mount(source=f"{HOST_REPO}/src/quality", target="/opt/quality",
                      type="bind", read_only=True),
            ],
            environment={
                "S3_ENDPOINT": "http://localstack:4566",
                "S3_ACCESS_KEY": "minioadmin",
                "S3_SECRET_KEY": "minioadmin",
                "BUCKET": "lakehouse",
            },
            **DOCKER_COMMON,
        )

    quality_bronze = quality_gate("bronze")
    quality_silver = quality_gate("silver")

    # 2. The exact Spark container you run by hand, launched by Airflow.
    silver_spark = DockerOperator(
        task_id="silver_spark",
        image=SPARK_IMAGE,
        user="0:0",
        mounts=[
            Mount(source=f"{HOST_REPO}/src/silver", target="/opt/job",
                  type="bind", read_only=True),
            Mount(source=f"{HOST_REPO}/data/ivy", target="/tmp/.ivy",
                  type="bind"),
        ],
        environment={
            "S3_ENDPOINT": "http://localstack:4566",
            "S3_ACCESS_KEY": "minioadmin",
            "S3_SECRET_KEY": "minioadmin",
            "BUCKET": "lakehouse",
        },
        command=(
            "/opt/spark/bin/spark-submit --master local[2] "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4 "
            "--conf spark.jars.ivy=/tmp/.ivy "
            "--conf spark.ui.showConsoleProgress=false "
            "/opt/job/silver_job.py"
        ),
        **DOCKER_COMMON,
    )

    # 3. Silver -> Postgres, run inside the Airflow container.
    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command=(
            "MINIO_ENDPOINT=http://localstack:4566 "
            "WAREHOUSE_DSN='host=postgres port=5432 dbname=lakehouse "
            "user=lakehouse password=lakehouse' "
            "python /opt/airflow/repo/scripts/load_silver_to_warehouse.py"
        ),
    )

    # 4 + 5. The dbt image you built, launched by Airflow: build, then test.
    dbt_mounts = [
        Mount(source=f"{HOST_REPO}/dbt", target="/usr/app", type="bind"),
        Mount(source=f"{HOST_REPO}/dbt/profiles", target="/root/.dbt",
              type="bind"),
    ]
    dbt_run = DockerOperator(
        task_id="dbt_run",
        image=DBT_IMAGE,
        command="run",
        mounts=dbt_mounts,
        environment={"DBT_HOST": "postgres"},
        **DOCKER_COMMON,
    )
    dbt_test = DockerOperator(
        task_id="dbt_test",
        image=DBT_IMAGE,
        command="test",
        mounts=dbt_mounts,
        environment={"DBT_HOST": "postgres"},
        **DOCKER_COMMON,
    )

    # The pipeline, as one readable line - now with its two quality gates:
    (bronze_ingest >> quality_bronze >> silver_spark >> quality_silver
     >> load_warehouse >> dbt_run >> dbt_test)
