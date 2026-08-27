-- 1. Speed distribution per rover
SELECT rover_id,
       approx_percentile(speed_kmh, 0.5) AS median_speed,
       approx_percentile(speed_kmh, 0.9) AS p90_speed
FROM iceberg.cityrover.telemetry_raw
GROUP BY rover_id;

-- 2. Daily average speed per rover
SELECT roverid, event_date, AVG(speed_kmh)
FROM iceberg.cityrover.telemetry_raw
GROUP BY rover_id, event_date
ORDER BY event_date DESC;

