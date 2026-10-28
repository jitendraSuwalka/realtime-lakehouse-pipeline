-- Merchant dimension: one row per merchant, LATEST attributes win.
-- Why not a plain select distinct? Because event streams can carry
-- attribute drift (the dbt unique test caught exactly that on the first
-- orchestrated run). Ranking by event_time and keeping row 1 makes this a
-- type-1 dimension: current attributes, guaranteed unique, whatever
-- upstream does.
with ranked as (
    select
        merchant_id,
        merchant_name,
        merchant_country,
        merchant_category,
        row_number() over (
            partition by merchant_id
            order by event_time desc
        ) as rn
    from {{ ref('stg_payments') }}
)

select
    merchant_id,
    merchant_name,
    merchant_country,
    merchant_category
from ranked
where rn = 1
