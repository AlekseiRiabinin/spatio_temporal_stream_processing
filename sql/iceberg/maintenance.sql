-- 1. Compact small files
ALTER TABLE iceberg.cityrover.telemetry_raw EXECUTE optimize;

-- 2. Compact only recent partitions
-- (cheaper, if partitioned by date)
ALTER TABLE iceberg.cityrover.telemetry_raw EXECUTE optimize
WHERE event_date >= DATE '2026-08-24';

-- 3. Expire old snapshots
-- (reclaim storage — keep e.g. last 7 days)
ALTER TABLE iceberg.cityrover.telemetry_raw EXECUTE expire_snapshots(retention_threshold => '7d');

-- 4. Remove orphaned files
-- (files not referenced by any snapshot, e.g. from failed writes)
ALTER TABLE iceberg.cityrover.telemetry_raw EXECUTE remove_orphan_files(retention_threshold => '3d');

-- 5. Fixing the duplicates in place
DELETE FROM iceberg.cityrover.telemetry_raw t
WHERE t.epoch_ms IN (
  SELECT epoch_ms FROM (
    SELECT epoch_ms,
           ROW_NUMBER() OVER (PARTITION BY rover_id, event_time ORDER BY epoch_ms) AS rn
    FROM iceberg.cityrover.telemetry_raw
  ) x WHERE rn > 1
);

-- 6. Rollback to a known-good snapshot
-- (if a bad batch got committed)
ALTER TABLE iceberg.cityrover.telemetry_raw EXECUTE rollback_to_snapshot(885918832154791160);
