{{ config(materialized='incremental', database='gold', unique_key='sunshine_id') }}

WITH monthly_agg AS (
    SELECT
        toStartOfMonth(date) AS month_start,
        round(avg(sunshine_hours), 2) AS avg_sunshine
    FROM model_silver.sunshine_cleaned
    WHERE toYear(date) >= 2021
    GROUP BY toStartOfMonth(date)
    HAVING avg_sunshine IS NOT NULL
)

SELECT
    cityHash64(
        concat(
            toString(month_start),
            toString(avg_sunshine)
        )
    ) AS sunshine_id,
    formatDateTime(month_start, '%Y%m') AS date_id,
    avg_sunshine,
FROM monthly_agg
ORDER BY month_start
