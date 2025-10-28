SELECT
    d.staid,
    d.date,
    d.ss_raw / 10.0 AS sunshine_hours,
    d.q_ss AS valid_flag,
    s.station_name,
    s.country
FROM bronze.sunshine_daily AS d
LEFT JOIN bronze.sunshine_sources AS s
    ON d.staid = s.staid
WHERE d.q_ss IN (0, 1)