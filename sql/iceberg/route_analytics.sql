-- 1. Most frequently used edges
SELECT edge_id, COUNT(*) AS cnt
FROM iceberg.cityrover.telemetry_raw
GROUP BY edge_id
ORDER BY cnt DESC
LIMIT 20;

-- 2. Most common routes
SELECT route_id, COUNT(*) AS cnt
FROM iceberg.cityrover.telemetry_raw
GROUP BY route_id
ORDER BY cnt DESC
LIMIT 20;

-- 3. Average speed per route
SELECT route_id, AVG(speed_kmh) AS avg_speed
FROM iceberg.cityrover.telemetry_raw
GROUP BY route_id
ORDER BY avg_speed DESC;


