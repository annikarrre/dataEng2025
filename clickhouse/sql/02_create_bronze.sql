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

CREATE TABLE IF NOT EXISTS bronze.sunshine_daily
(
  staid UInt32,
  souid UInt32,
  date Date,
  ss_raw Int32,
  q_ss UInt8,
  _ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (staid, date);
