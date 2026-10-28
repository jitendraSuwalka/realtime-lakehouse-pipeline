-- Merchant dimension: one row per merchant with its descriptive attributes.
-- (In a real platform this would be enriched from a merchant master system;
-- here the attributes travel on the events, so distinct is enough.)
select distinct
    merchant_id,
    merchant_name,
    merchant_country,
    merchant_category
from {{ ref('stg_payments') }}
