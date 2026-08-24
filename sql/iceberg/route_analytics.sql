-- 1. Most frequently used edges
SELECT edgeid, COUNT(*) AS cnt
FROM iceberg.cityrover.telemetry_raw
GROUP BY edgeid
ORDER BY cnt DESC
LIMIT 20;

-- 2. Most common routes
SELECT routeid, COUNT(*) AS cnt
FROM iceberg.cityrover.telemetry_raw
GROUP BY routeid
ORDER BY cnt DESC
LIMIT 20;

-- 3. Average speed per route
SELECT routeid, AVG(speed_kmh) AS avg_speed
FROM iceberg.cityrover.telemetry_raw
GROUP BY routeid
ORDER BY avg_speed DESC;


