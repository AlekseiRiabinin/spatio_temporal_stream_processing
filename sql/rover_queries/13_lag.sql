-- Show the previous position of every rover
SELECT
    rover_id,
    timestamp,
    latitude,
    longitude,

    LAG(latitude) OVER (
        PARTITION BY rover_id
        ORDER BY timestamp
    ) AS previous_latitude,

    LAG(longitude) OVER (
        PARTITION BY rover_id
        ORDER BY timestamp
    ) AS previous_longitude

FROM rover_events;


-- Calculate the change in speed between consecutive observations
WITH previous AS (
    SELECT
        rover_id,
        timestamp,
        speed,

        LAG(speed) OVER (
            PARTITION BY rover_id
            ORDER BY timestamp
        ) AS previous_speed

    FROM rover_events
)
SELECT
    rover_id,
    timestamp,
    speed,
    previous_speed,
    speed - previous_speed AS speed_change
FROM previous;

-- current_speed
--       -
-- previous_speed
--       =
-- speed_change
