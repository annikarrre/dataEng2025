{{ config(materialized='incremental', database='gold', unique_key='sunshine_id') }}

WITH monthly_agg AS (
    SELECT
        toStartOfMonth(date) AS month_start
    FROM model_silver.sunshine_cleaned
    WHERE toYear(date) >= 2021
    GROUP BY toStartOfMonth(date)
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
        FROM model_silver.sunshine_cleaned
        GROUP BY month_start, staid
    )
    GROUP BY month_start
)

SELECT
    cityHash64(
        concat(
            toString(m.month_start),
            toString(round(a.avg_station_month, 2))
        )
    ) AS sunshine_id,
    formatDateTime(m.month_start, '%Y%m') AS date_id,
    round(a.avg_station_month, 2) AS avg_station_monthly_hours
FROM monthly_agg AS m
LEFT JOIN avg_station_month AS a
    ON a.month_start = m.month_start
ORDER BY m.month_start