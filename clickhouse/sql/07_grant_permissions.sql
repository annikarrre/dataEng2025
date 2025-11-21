GRANT SELECT ON model_gold.dim_medications TO analyst_full;
GRANT SELECT (medication_id, year, month, ingredient, atc_code, package_code, package_label, total_packages, total_amount, health_insurance_amount, amount_over_reference_price, total_prescriptions, total_persons) ON model_gold.dim_medications TO analyst_limited;
GRANT SELECT ON model_gold.v_medications_masked TO analyst_limited;
GRANT SELECT ON model_gold.v_medications_full TO analyst_full;