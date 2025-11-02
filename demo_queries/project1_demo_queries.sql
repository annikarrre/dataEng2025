-- Q1: How does temperature affect medication sales?
SELECT d.year, d.month, w.avg_temp, SUM(f.quantity_sold) AS total_sold
FROM fact_medication_sales f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_weather w ON f.weather_id = w.weather_id
GROUP BY d.year, d.month, w.avg_temp
ORDER BY d.year, d.month;

-- Q2: Seasonal spikes by medication type
SELECT d.season, m.medication_type, SUM(f.quantity_sold) AS total_sold
FROM fact_medication_sales f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_medication m ON f.medication_id = m.medication_id
GROUP BY d.season, m.medication_type
ORDER BY total_sold DESC;
