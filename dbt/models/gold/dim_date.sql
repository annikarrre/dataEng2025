WITH months AS (
    SELECT
        toStartOfMonth(addMonths('2021-01-01', number)) AS month_start
    FROM numbers(60)  -- 60 months = 5 years (2021–2025)
)

SELECT
    formatDateTime(month_start, '%Y%m') AS date_id,
    toYear(month_start) AS year,
    toMonth(month_start) AS month,
    toQuarter(month_start) AS quarter,
    formatDateTime(month_start, '%M') AS month_name,
    concat(toString(toYear(month_start)), '-', formatDateTime(month_start, '%m')) AS year_month_label,
    CASE
        WHEN toMonth(month_start) IN (12, 1, 2) THEN 'Winter'
        WHEN toMonth(month_start) IN (3, 4, 5) THEN 'Spring'
        WHEN toMonth(month_start) IN (6, 7, 8) THEN 'Summer'
        WHEN toMonth(month_start) IN (9, 10, 11) THEN 'Autumn'
        ELSE 'Unknown'
    END AS season
FROM months
ORDER BY month_start