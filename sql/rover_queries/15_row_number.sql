-- Find the latest event for every rover
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY rover_id
            ORDER BY timestamp DESC
        ) AS rn
    FROM rover_events
)
SELECT
    *
FROM ranked
WHERE rn = 1;


-- Find the three fastest observations for each rover
WITH ranked AS (
    SELECT
        rover_id,
        timestamp,
        speed,

        ROW_NUMBER() OVER (
            PARTITION BY rover_id
            ORDER BY speed DESC
        ) AS rn

    FROM rover_events
)
SELECT
    rover_id,
    timestamp,
    speed
FROM ranked
WHERE rn <= 3;
