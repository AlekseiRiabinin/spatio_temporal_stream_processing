-- Produce ML features
WITH features AS (
    SELECT
        rover_id,
        timestamp,
        speed,
        distance_meters,

        AVG(speed) OVER (
            PARTITION BY rover_id
            ORDER BY timestamp
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS avg_speed_5,

        MAX(speed) OVER (
            PARTITION BY rover_id
            ORDER BY timestamp
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS max_speed_5,

        SUM(distance_meters) OVER (
            PARTITION BY rover_id
            ORDER BY timestamp
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS distance_10,

        LAG(speed) OVER (
            PARTITION BY rover_id
            ORDER BY timestamp
        ) AS previous_speed

    FROM rover_events
)
SELECT
    rover_id,
    timestamp,
    avg_speed_5,
    max_speed_5,
    distance_10,
    speed - previous_speed AS speed_change
FROM features;
