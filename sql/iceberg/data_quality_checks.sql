-- 1. Duplicate detection
-- (confirming/quantifying what we saw earlier)
SELECT roverid, event_time, COUNT(*) AS cnt
FROM iceberg.cityrover.telemetry_raw
GROUP BY roverid, event_time
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

-- 2. Duplicates isolated to a specific append
-- (using the diff pattern from before, but counting)
SELECT roverid, event_time, COUNT(*) AS cnt
FROM iceberg.cityrover.telemetry_raw
FOR VERSION AS OF 1442785148312161673
GROUP BY roverid, event_time
HAVING COUNT(*) > 1
LIMIT 10;

-- 3. Gaps in telemetry per rover
-- (missed heartbeats — useful if you expect ~5s cadence)
WITH ordered AS (
  SELECT roverid, event_time,
         LAG(event_time) OVER (PARTITION BY roverid ORDER BY event_time) AS prev_time
  FROM iceberg.cityrover.telemetry_raw
)
SELECT roverid, prev_time, event_time,
       (event_time - prev_time) AS gap
FROM ordered
WHERE (event_time - prev_time) > INTERVAL '10' SECOND
ORDER BY gap DESC;

-- 4. Battery/speed sanity check
-- (out-of-range values)
SELECT *
FROM iceberg.cityrover.telemetry_raw
WHERE battery NOT BETWEEN 0 AND 100
   OR speed < 0
   OR lat NOT BETWEEN -90 AND 90
   OR lon NOT BETWEEN -180 AND 180;

-- 5. Rover activity summary:
SELECT rover_id,
       COUNT(*) AS pings,
       MIN(event_time) AS first_seen,
       MAX(event_time) AS last_seen,
       AVG(speed) AS avg_speed,
       AVG(battery) AS avg_battery
FROM iceberg.cityrover.telemetry_raw
GROUP BY roverid
ORDER BY pings DESC;


