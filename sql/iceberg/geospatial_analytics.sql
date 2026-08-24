-- 1. Bounding box of rover movement
SELECT
  MIN(lat) AS min_lat,
  MAX(lat) AS max_lat,
  MIN(lon) AS min_lon,
  MAX(lon) AS max_lon
FROM iceberg.cityrover.telemetry_raw;

-- 2. Detect stationary rovers
SELECT roverid, COUNT(*) AS stationary_points
FROM iceberg.cityrover.telemetry_raw
WHERE speed_kmh < 1
GROUP BY roverid
ORDER BY stationary_points DESC;


