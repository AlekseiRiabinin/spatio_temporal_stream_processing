-- Suppose the rover should send an event every 10 seconds
-- Find gaps greater than 30 seconds
WITH events AS (
    SELECT
        rover_id,
        timestamp,

        LAG(timestamp) OVER (
            PARTITION BY rover_id
            ORDER BY timestamp
        ) AS previous_timestamp

    FROM rover_events
)
SELECT
    rover_id,
    previous_timestamp,
    timestamp,
    timestamp - previous_timestamp AS gap
FROM events
WHERE timestamp - previous_timestamp > INTERVAL '30 seconds';


-- Suppose: speed < 1 km/h means the rover is stopped
-- Identify stopped observations
SELECT
    rover_id,
    timestamp,
    speed,
    CASE
        WHEN speed < 1 THEN 1
        ELSE 0
    END AS is_stopped
FROM rover_events;


-- Calculate stop duration
WITH ordered AS (
    SELECT
        rover_id,
        timestamp,
        speed,

        LEAD(timestamp) OVER (
            PARTITION BY rover_id
            ORDER BY timestamp
        ) AS next_timestamp

    FROM rover_events
)
SELECT
    rover_id,
    timestamp,
    next_timestamp,
    next_timestamp - timestamp AS duration
FROM ordered
WHERE speed < 1;
