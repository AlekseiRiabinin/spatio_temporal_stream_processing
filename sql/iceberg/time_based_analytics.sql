-- 1. Daily record count    
SELECT event_date, COUNT(*)
FROM iceberg.cityrover.telemetry_raw
GROUP BY event_date
ORDER BY event_date DESC;

-- 2. Hourly speed patterns
SELECT
  date_trunc('hour', event_time) AS hour,
  AVG(speed_kmh) AS avg_speed
FROM iceberg.cityrover.telemetry_raw
GROUP BY 1
ORDER BY hour DESC
LIMIT 20;
