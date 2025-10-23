CREATE DATABASE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.sunshine_sources
(
  staid UInt32,
  souid UInt32,
  station_name String,
  country FixedString(2),
  lat String,
  lon String,
  elevation Int32,
  begin UInt32,
  end UInt32,
  parname String,
  _ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree()
ORDER BY (staid, souid);

CREATE TABLE bronze.sunshine_daily (
  staid UInt32,
  souid UInt32,
  date  Date32,
  ss_raw Nullable(Int32),
  q_ss  Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY (souid, date);