{{ config(materialized='incremental', database='gold', unique_key='weather_id') }}

WITH monthly_agg AS (
    SELECT
        toStartOfMonth(date) AS month_start,
        round(avg(avg_temp_c), 2) AS avg_temp,
        round(sum(total_precip_mm), 2) AS precipitation,
        round(avg(avg_wind_ms), 2) AS wind_speed,
        round(avg(avg_humidity), 2) AS humidity
    FROM model_silver.weather_cleaned
    WHERE toYear(date) >= 2021
      AND avg_temp_c IS NOT NULL
      AND total_precip_mm IS NOT NULL
      AND avg_wind_ms IS NOT NULL
      AND avg_humidity IS NOT NULL
    GROUP BY toStartOfMonth(date)
)

SELECT
    cityHash64(
        concat(
            toString(month_start),
            toString(avg_temp),
            toString(precipitation),
            toString(wind_speed),
            toString(humidity)
        )
    ) AS weather_id,
    formatDateTime(month_start, '%Y%m') AS date_id,
    avg_temp,
    precipitation,
    wind_speed,
    humidity
FROM monthly_agg
ORDER BY month_start