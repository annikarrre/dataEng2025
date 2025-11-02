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

--Q5: How many of various medications should be supplied per period?
SELECT
    d.month,
    AVG(f.total_packages) AS avg_quantity_per_month,
    SUM(f.total_packages) AS total_quantity_all_years,
    COUNT(DISTINCT d.year) AS years_of_data
FROM
    model_gold.fact_medication_sales f
INNER JOIN
    model_gold.dim_medications m ON f.medication_id = m.medication_id
INNER JOIN
    model_gold.dim_date d ON f.date_id = d.date_id
GROUP BY
    d.month
ORDER BY
    d.month;