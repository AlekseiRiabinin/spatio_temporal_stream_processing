-- Calculate the average speed over the previous 5 observations
SELECT
    rover_id,
    timestamp,
    speed,

    AVG(speed) OVER (
        PARTITION BY rover_id
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS avg_speed_last_5

FROM rover_events;
