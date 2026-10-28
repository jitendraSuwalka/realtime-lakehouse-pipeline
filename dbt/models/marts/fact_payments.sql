-- The fact table: one row per payment event (the finest grain we have).
-- Facts carry the measures (amount) and the keys to the dimensions;
-- the descriptive text lives in the dims. This shape - one fact, pointed
-- at by small dims - is the "star schema".
select
    event_id,
    event_date,
    event_time,
    merchant_id,
    customer_id,
    amount,
    currency,
    payment_method,
    status
from {{ ref('stg_payments') }}
