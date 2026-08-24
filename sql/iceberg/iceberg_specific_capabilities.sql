-- 1. Find valid snapshot IDs
SELECT snapshot_id, committed_at, operation
FROM iceberg.cityrover."telemetry_raw$snapshots"
ORDER BY committed_at DESC;

-- 2. Time travel (query past snapshot)
SELECT *
FROM iceberg.cityrover.telemetry_raw
FOR VERSION AS OF 885918832154791160;

-- 3. Time travel by timestamp instead of snapshot ID
SELECT *
FROM iceberg.cityrover.telemetry_raw
FOR TIMESTAMP AS OF TIMESTAMP '2026-08-24 03:08:12.088 Europe/Sofia';
