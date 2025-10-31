SELECT
    station,
    toDate(ts) AS date,
    CAST(round(avgIf(t_air_c, t_air_c IS NOT NULL AND isFinite(t_air_c)), 2) AS Decimal(5,2)) AS avg_temp_c,
    CAST(round(minIf(t_min_c, t_min_c IS NOT NULL AND isFinite(t_min_c)), 2) AS Decimal(5,2)) AS min_temp_c,
    CAST(round(maxIf(t_max_c, t_max_c IS NOT NULL AND isFinite(t_max_c)), 2) AS Decimal(5,2)) AS max_temp_c,
    CAST(round(sumIf(precip_mm, precip_mm IS NOT NULL AND isFinite(precip_mm)), 2) AS Decimal(7,2)) AS total_precip_mm,
    CAST(round(avgIf(wind_ms, wind_ms IS NOT NULL AND isFinite(wind_ms)), 2) AS Decimal(5,2)) AS avg_wind_ms,
    CAST(round(avgIf(wind_gust_ms, wind_gust_ms IS NOT NULL AND isFinite(wind_gust_ms)), 2) AS Decimal(5,2)) AS avg_wind_gust_ms,
    CAST(round(avgIf(humidity, humidity IS NOT NULL AND isFinite(humidity)), 2) AS Decimal(5,2)) AS avg_humidity
FROM bronze.weather_hourly
WHERE t_air_c IS NOT NULL AND isFinite(t_air_c)
  AND t_min_c IS NOT NULL AND isFinite(t_min_c)
  AND t_max_c IS NOT NULL AND isFinite(t_max_c)
  AND precip_mm IS NOT NULL AND isFinite(precip_mm)
  AND wind_ms IS NOT NULL AND isFinite(wind_ms)
  AND wind_gust_ms IS NOT NULL AND isFinite(wind_gust_ms)
  AND humidity IS NOT NULL AND isFinite(humidity)
GROUP BY
    station, date