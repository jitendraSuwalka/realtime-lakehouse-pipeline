# dbt runner image.
# Why build our own instead of pip-installing dbt on the laptop?
# Same reason as Spark: the laptop's Python 3.14 is newer than dbt supports,
# so the container carries its own Python 3.12. Building a small image from
# a Dockerfile is also exactly how dbt ships in production CI pipelines.
FROM python:3.12-slim

RUN pip install --no-cache-dir dbt-postgres==1.9.0

WORKDIR /usr/app
ENTRYPOINT ["dbt"]
