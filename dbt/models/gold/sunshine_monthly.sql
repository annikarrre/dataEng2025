-- models/gold/sunshine_monthly.sql
-- Estonia-level monthly sunshine (all stations combined), rounded to 2 decimals.

WITH
per_month AS (
    SELECT
        toStartOfMonth(date) AS month_start,
        sum(sunshine_hours)  AS sum_sunshine,
        avg(sunshine_hours)  AS avg_daily,
        count()              AS obs_count,
        uniqExact(staid)     AS station_count
    FROM silver.sunshine_cleaned
    GROUP BY month_start
),
avg_station_month AS (
    SELECT
        month_start,
        avg(station_month_total_hours) AS avg_station_month
    FROM (
        SELECT
            toStartOfMonth(date) AS month_start,
            staid,
            sum(sunshine_hours)  AS station_month_total_hours
        FROM silver.sunshine_cleaned
        GROUP BY month_start, staid
    )
    GROUP BY month_start
)

SELECT
    p.month_start,
    round(p.sum_sunshine, 2)      AS total_sunshine_hours_all_stations,
    round(p.avg_daily, 2)         AS avg_daily_sunshine_hours_estonia,
    round(a.avg_station_month, 2) AS avg_station_monthly_hours,
    p.station_count               AS station_count_in_month,
    p.obs_count                   AS station_day_observations
FROM per_month p
LEFT JOIN avg_station_month a ON p.month_start = a.month_start
ORDER BY p.month_start
