{{ config(materialized='incremental', database='gold', unique_key='weather_id') }}

WITH monthly_agg AS (
    SELECT
        toStartOfMonth(date) AS month_start,
        round(avg(avg_temp_c), 2) AS avg_monthly_temp,
        round(sum(total_precip_mm), 2) AS avg_monthly_precipitation,
        round(avg(avg_wind_ms), 2) AS avg_monthly_wind_speed,
        round(avg(avg_humidity), 2) AS avg_monthly_humidity
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
            toString(avg_monthly_temp),
            toString(avg_monthly_precipitation),
            toString(avg_monthly_wind_speed),
            toString(avg_monthly_humidity)
        )
    ) AS weather_id,
    formatDateTime(month_start, '%Y%m') AS date_id,
    avg_monthly_temp,
    avg_monthly_precipitation,
    avg_monthly_wind_speed,
    avg_monthly_humidity
FROM monthly_agg
ORDER BY month_start