{{ config(materialized='table', database='gold') }}

WITH base AS (
    SELECT
        year,
        month,
        trim(splitByChar(' ', ingredient_name)[2]) AS ingredient,
        trim(splitByChar(' ', ingredient_name)[1]) AS atc_code,
        trim(splitByChar(' ', package_name)[1]) AS package_code,
        trim(arrayStringConcat(arraySlice(splitByChar(' ', package_name), 2, length(splitByChar(' ', package_name)) - 1), ' ')) AS package_label,
        total_packages,
        total_amount,
        health_insurance_amount,
        copay_with_threshold,
        copay_without_threshold,
        amount_over_reference_price,
        total_prescriptions,
        total_persons
    FROM model_silver.medications_cleaned
    WHERE ingredient_name IS NOT NULL
)

SELECT
    cityHash64(
        concat(
            toString(year),
            toString(month),
            ingredient,
            atc_code,
            package_code,
            package_label
        )
    ) AS medication_id,
    year,
    month,
    ingredient,
    atc_code,
    package_code,
    package_label,
    sum(total_packages) AS total_packages,
    sum(total_amount) AS total_amount,
    sum(health_insurance_amount) AS health_insurance_amount,
    sum(copay_with_threshold) AS copay_with_threshold,
    sum(copay_without_threshold) AS copay_without_threshold,
    sum(amount_over_reference_price) AS amount_over_reference_price,
    sum(total_prescriptions) AS total_prescriptions,
    sum(total_persons) AS total_persons
FROM base
GROUP BY
    year,
    month,
    ingredient,
    atc_code,
    package_code,
    package_label
ORDER BY
    year,
    month,
    ingredient,
    atc_code,
    package_code,
    package_label