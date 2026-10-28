-- Staging: one thin, typed view over the raw source. No business logic -
-- just a stable, renamed contract for the marts to build on.
with source as (

    select * from {{ source('raw', 'silver_payments') }}

)

select
    event_id,
    event_time,
    event_date,
    merchant_id,
    merchant_name,
    merchant_country,
    merchant_category,
    customer_id,
    amount,
    currency,
    payment_method,
    status,
    kafka_partition,
    kafka_offset,
    ingested_at,
    ingest_date
from source
