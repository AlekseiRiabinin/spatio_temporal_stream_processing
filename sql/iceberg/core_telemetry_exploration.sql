-- 1. Latest records per rover
SELECT rover_id, ts, lat, lon, speed_kmh
FROM iceberg.cityrover.telemetry_raw
ORDER BY ts DESC
LIMIT 20;

-- 2. Count records per rover
SELECT rover_id, COUNT(*) AS cnt
FROM iceberg.cityrover.telemetry_raw
GROUP BY rover_id
ORDER BY cnt DESC;

-- 3. Average speed per rover
SELECT roverid, AVG(speed_kmh) AS avg_speed
FROM iceberg.cityrover.telemetry_raw
GROUP BY rover_id
ORDER BY avg_speed DESC;

