-- Keep only the latest version of each event
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY timestamp DESC
        ) AS rn
    FROM rover_events
)
SELECT *
FROM ranked
WHERE rn = 1;
