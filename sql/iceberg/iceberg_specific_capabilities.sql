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

-- 4. Snapshot history with size/record deltas
-- (see how each append grew the table)
SELECT snapshot_id, parent_id, operation, committed_at,
       element_at(summary, 'added-records') AS added_records,
       element_at(summary, 'added-files-size') AS added_bytes
FROM iceberg.cityrover."telemetry_raw$snapshots"
ORDER BY committed_at DESC;

-- 5. Data files per snapshot
-- (spot small-file problems from frequent 5s appends)
SELECT file_path, file_format, record_count, file_size_in_bytes
FROM iceberg.cityrover."telemetry_raw$files"
ORDER BY file_size_in_bytes ASC
LIMIT 20;

-- 6. Partition summary
-- (if the table is partitioned, e.g. by event_date)
SELECT * FROM iceberg.cityrover."telemetry_raw$partitions"
ORDER BY record_count DESC;

-- 7. Table properties / format version:
SHOW CREATE TABLE iceberg.cityrover.telemetry_raw;

-- 8. Refs (branches/tags, if used them):
SELECT * FROM iceberg.cityrover."telemetry_raw$refs";



