-- models/gold/sunshine_monthly.sql
-- Aggregates daily sunshine into monthly totals and averages.

with base as (
    select
        staid,
        toStartOfMonth(date) as month,
        any(station_name) as station_name,
        any(country) as country,
        sum(sunshine_hours) as total_sunshine_hours,
        avg(sunshine_hours) as avg_sunshine_hours,
        count() as days_count
    from silver.sunshine_cleaned
    group by staid, toStartOfMonth(date)
)

select *
from base
order by staid, month
