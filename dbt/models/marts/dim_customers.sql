-- Customer dimension. Deliberately thin: the events only carry the ID.
-- In production this is where CRM attributes (segment, signup date,
-- country) would join on - the dimension is the landing pad for them.
select distinct
    customer_id
from {{ ref('stg_payments') }}
