CREATE OR REPLACE VIEW model_gold.v_medications_masked AS
SELECT
    medication_id,
    year,
    month,
    ingredient,
    atc_code,
    replaceRegexpAll(package_code, '^([0-9]{2})[0-9]+([0-9]{2})$', '\\1***\\2') AS package_code_masked,
    cityHash64(package_label) AS package_label_masked,
    total_packages,
    total_amount,
    health_insurance_amount,
    amount_over_reference_price,
    concat(
        toString(intDiv(toUInt64(total_prescriptions), 1000) * 1000),
        '–',
        toString(intDiv(toUInt64(total_prescriptions), 1000) * 1000 + 999)
    ) AS total_prescriptions_range,
    total_persons
FROM model_gold.dim_medications;

CREATE OR REPLACE VIEW model_gold.v_medications_full AS
SELECT * FROM model_gold.dim_medications;

