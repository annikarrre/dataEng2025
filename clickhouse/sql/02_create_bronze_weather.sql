CREATE DATABASE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.weather_hourly (
  station LowCardinality(String),
  ts      DateTime,
  t_air_c Nullable(Float32),
  t_min_c Nullable(Float32),
  t_max_c Nullable(Float32),
  wind_dir_deg Nullable(UInt16),
  wind_ms      Nullable(Float32),
  wind_gust_ms Nullable(Float32),
  precip_mm    Nullable(Float32)
)
ENGINE = MergeTree
ORDER BY (station, ts);
