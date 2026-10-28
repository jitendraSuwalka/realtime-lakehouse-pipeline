-- Date dimension: calendar attributes for every date that appears in the
-- facts. Lets analysts group by month/weekday without date functions.
-- (Small today - it grows a row per trading day, exactly as it should.)
with dates as (

    select distinct event_date as date_day
    from {{ ref('stg_payments') }}

)

select
    date_day,
    extract(year from date_day)::int        as year,
    extract(month from date_day)::int       as month,
    trim(to_char(date_day, 'Month'))        as month_name,
    extract(isodow from date_day)::int      as day_of_week,
    trim(to_char(date_day, 'Day'))          as day_name,
    extract(isodow from date_day) in (6, 7) as is_weekend
from dates
