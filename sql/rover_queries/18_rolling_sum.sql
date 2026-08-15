-- Calculate the total distance covered over the last 10 observations
SELECT
    rover_id,
    timestamp,
    distance_meters,

    SUM(distance_meters) OVER (
        PARTITION BY rover_id
        ORDER BY timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS distance_last_10

FROM rover_events;


-- SQL
--  ↓
-- rolling feature
--  ↓
-- ML
