--Q1: How does monthly average temperature affect the volume of medication purchases?
SELECT
    d.year,
    d.month,
    f.avg_temp,
    SUM(f.total_packages) AS total_quantity
FROM model_gold.fact_medication_sales AS f
JOIN model_gold.dim_date AS d
    ON f.date_id = d.date_id
JOIN model_gold.dim_weather AS w
    ON f.weather_id = w.weather_id
GROUP BY
    d.year,
    d.month,
    f.avg_temp
ORDER BY
    d.year,
    d.month;

--Q2: Are there specific types of medications (cold/flu, allergy, depression) that show seasonal spikes related to weather conditions?
SELECT
    m.ingredient AS medication_type,
    d.year,
    d.month,
    AVG(f.avg_temp) AS avg_temp,
    SUM(f.total_packages) AS total_quantity
FROM model_gold.fact_medication_sales AS f
JOIN model_gold.dim_medications AS m
    ON f.medication_id = m.medication_id
JOIN model_gold.dim_date AS d
    ON f.date_id = d.date_id
JOIN model_gold.dim_weather AS w
    ON f.weather_id = w.weather_id
GROUP BY
    m.ingredient,  -- medication type
    d.year,
    d.month
ORDER BY
    total_quantity DESC;

--Q3: Does extreme weather (heatwaves, cold) correlate with higher prescription rates for certain diagnoses?

SELECT
    m.ingredient AS medication_name,
    CASE
        WHEN w.avg_monthly_temp < -5 THEN 'extreme_cold'
        WHEN w.avg_monthly_temp > 21 THEN 'extreme_heat'
        ELSE 'normal'
    END AS weather_type,
    COUNT(DISTINCT w.date_id) AS month_count,
    SUM(f.total_prescriptions) AS total_prescriptions,
    round(SUM(f.total_prescriptions) / COUNT(DISTINCT w.date_id), 2) AS avg_prescriptions_per_month
FROM model_gold.fact_medication_sales AS f
JOIN model_gold.dim_medications AS m
    ON f.medication_id = m.medication_id
JOIN model_gold.dim_weather AS w
    ON f.weather_id = w.weather_id
GROUP BY
    m.ingredient,
    weather_type
ORDER BY
    m.ingredient,
    weather_type;

--Q4: Can weather patterns be used to forecast future medication demand (for example flu outbreaks during colder months, depression increases during colder months)?

SELECT
    d.year,
    d.month,
    round(avg(w.avg_monthly_temp), 2)        AS avg_temp_c,
    round(avg(s.avg_station_monthly_hours), 2) AS avg_sunshine_hours,
    SUM(f.total_packages)                   AS total_packages_sold,
    SUM(f.total_prescriptions)              AS total_prescriptions
FROM model_gold.fact_medication_sales AS f
JOIN model_gold.dim_date AS d
    ON f.date_id = d.date_id
JOIN model_gold.dim_weather AS w
    ON f.weather_id = w.weather_id
JOIN model_gold.dim_sunshine AS s
    ON f.sunshine_id = s.sunshine_id
GROUP BY
    d.year,
    d.month
ORDER BY
    d.year ASC,
    d.month ASC;

--Q5: How many of various medications should be supplied per period?
SELECT
    d.month,
    m.package_label,
    AVG(f.total_packages) AS avg_packages_per_month
FROM
    model_gold.fact f
INNER JOIN
    model_gold.dim_medications m ON f.medication_id = m.medication_id
INNER JOIN
    model_gold.dim_date d ON f.m.date_id = d.date_id
GROUP BY
    d.month, m.package_label
ORDER BY
    d.month, m.package_label;
