-- 1. Detect impossible speeds
SELECT *
FROM iceberg.cityrover.telemetry_raw
WHERE speed_kmh > 120;

-- 2. Detect sudden heading jumps
SELECT roverid, ts, heading
FROM iceberg.cityrover.telemetry_raw
WHERE heading > 350 OR heading < 10
ORDER BY ts DESC;


