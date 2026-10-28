# Airflow runner image.
# Base: the official Airflow image (Python 3.12 inside - same
# laptop-Python-too-new story as Spark and dbt).
# We add: the Docker provider (so Airflow can launch our Spark and dbt
# containers) and the bronze/loader dependencies (so those tasks can run
# directly inside Airflow).
# The --constraint line is Airflow's official trick: it pins every library
# to versions tested with this exact Airflow release, so nothing we install
# can accidentally upgrade or break Airflow itself.
FROM apache/airflow:2.10.3

RUN pip install --no-cache-dir \
    apache-airflow-providers-docker \
    confluent-kafka \
    boto3 \
    pyarrow \
    "psycopg[binary]" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt"
