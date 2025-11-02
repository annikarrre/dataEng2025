SELECT *
FROM {{ ref('fact_medication_sales') }}
WHERE total_packages <= 0