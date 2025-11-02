{{ config(materialized='table', database='gold') }}

WITH meds AS (
  SELECT
    m.medication_id,
    m.year,
    m.month,
    formatDateTime(
      toDate(concat(toString(m.year), '-', toString(m.month), '-01')),
      '%Y%m'
    ) AS date_id,
    m.total_packages,
    m.total_amount,
    m.total_prescriptions,
    m.total_persons
  FROM {{ ref('dim_medications') }} AS m
),

wx AS (
  SELECT
    weather_id,
    date_id,
    toFloat64(avg_monthly_temp)          AS avg_temp,
    toFloat64(avg_monthly_humidity)      AS humidity,
    toFloat64(avg_monthly_precipitation) AS precipitation
  FROM {{ ref('dim_weather') }}
),

sun AS (
  SELECT
    sunshine_id,
    date_id,
    toFloat64(avg_station_monthly_hours) AS avg_station_monthly_hours
  FROM {{ ref('dim_sunshine') }}
)

SELECT
  cityHash64(concat(toString(m.date_id), toString(m.medication_id))) AS fact_id,

  m.date_id,
  m.medication_id,

  w.weather_id,
  s.sunshine_id,

  w.avg_temp,
  w.humidity,
  w.precipitation,
  s.avg_station_monthly_hours AS avg_sunshine_hours,

  m.total_packages,
  m.total_amount,
  m.total_prescriptions,
  m.total_persons
FROM meds AS m
LEFT JOIN wx  AS w USING (date_id)
LEFT JOIN sun AS s USING (date_id)
ORDER BY m.date_id, m.medication_id
