CREATE DATABASE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.medications_monthly (
  year UInt16,
  month UInt8,
  month_date Date,
  ingredient LowCardinality(String),
  package String,
  persons UInt32,
  prescriptions UInt32,
  packages_count Float64,
  total_amount Float64,
  hk_amount Float64,
  over_ref_price Float64,
  copay_no_trh Float64,
  copay_with_trh Float64
)
ENGINE = MergeTree
ORDER BY (ingredient, package, month_date);