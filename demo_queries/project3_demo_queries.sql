-- 1.1 Retrieve the top 10 medication packages for February 2025, ranked by the total number of persons (full view)
SELECT
  year,
  month,
  package_code,
  ingredient,
  total_prescriptions,
  SUM(total_persons) AS persons
FROM model_gold.v_medications_full
WHERE year = 2025 AND month = 2
GROUP BY year, month, package_code, ingredient, total_prescriptions
ORDER BY year, persons DESC
LIMIT 10;

-- 1.2 Retrieve the top 10 medication packages for February 2025, ranked by the total number of persons (limited/masked view)
SELECT
  year,
  month,
  package_code_masked AS package_code,
  ingredient,
  total_prescriptions_range AS total_prescription,
  SUM(total_persons) AS persons
FROM model_gold.v_medications_masked
WHERE year = 2025 AND month = 2
GROUP BY year, month, package_code, ingredient, total_prescription
ORDER BY year, persons DESC
LIMIT 10;

-- 2.1 Calculate sales metrics per medication package for July 2025 and ranks them by the number of packages sold (full view)
SELECT
    year,
    month,
    package_label,
    SUM (total_packages) AS packages,
    SUM (total_amount) AS sales_amount,
    SUM (health_insurance_amount) AS reimbursed_amount,
    SUM (amount_over_reference_price) AS over_ref_amount
FROM model_gold.v_medications_full
WHERE year = 2025
and month = 7
GROUP BY year, month, package_label
ORDER BY year, month, packages DESC;

-- 2.2 Calculate sales metrics per medication package for July 2025 and ranks them by the number of packages sold (limited/masked view)
SELECT
    year,
    month,
    package_label_masked AS package_label,
    SUM(total_packages) AS packages,
    SUM (total_amount) AS sales_amount,
    SUM (health_insurance_amount) AS reimbursed_amount,
    SUM (amount_over_reference_price) AS over_ref_amount
FROM model_gold.v_medications_masked
WHERE year = 2025 and
month = 7
GROUP BY year, month, package_label_masked

-- 3.1 Find the top 10 ingredients by total packages sold in October 2024 (full view)
SELECT
    year,
    month,
    ingredient,
    SUM (total_packages) AS packages
FROM model_gold.v_medications_full
WHERE year = 2024 AND month = 10
GROUP BY year, month, ingredient
ORDER BY year, packages
DESC LIMIT 10;

-- 3.2 Find the top 10 ingredients by total packages sold in October 2024 (limited/masked view)
SELECT
    year,
    month,
    ingredient,
    SUM (total_packages) AS packages
    FROM model_gold.v_medications_masked
WHERE year = 2024 AND month = 10
GROUP BY year, month, ingredient
ORDER BY year, packages DESC
LIMIT 10;

