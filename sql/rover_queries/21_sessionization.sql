-- Create a new trip/session if the gap between consecutive events is greater than 5 minutes

-- First calculate the gap
WITH gaps AS (
    SELECT
        rover_id,
        timestamp,

        LAG(timestamp) OVER (
            PARTITION BY rover_id
            ORDER BY timestamp
        ) AS previous_timestamp

    FROM rover_events
),
-- Then identify session boundaries
boundaries AS (
    SELECT
        *,
        CASE
            WHEN previous_timestamp IS NULL
                OR timestamp - previous_timestamp > INTERVAL '5 minutes'
            THEN 1
            ELSE 0
        END AS new_session
    FROM gaps
)
-- Then create a session ID using a cumulative sum
SELECT
    rover_id,
    timestamp,

    SUM(new_session) OVER (
        PARTITION BY rover_id
        ORDER BY timestamp
    ) AS session_id

FROM boundaries;
