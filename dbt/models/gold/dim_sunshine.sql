SELECT
  toStartOfMonth(date)                       AS month_start,
  round(avg(sunshine_hours), 2)              AS avg_daily_sunshine_hours_estonia
FROM {{ ref('sunshine_cleaned') }}
WHERE date >= toDate('2021-01-01')
GROUP BY month_start
ORDER BY month_start
