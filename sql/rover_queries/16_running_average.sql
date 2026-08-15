-- Calculate the running average speed for every rover
SELECT
    rover_id,
    timestamp,
    speed,

    AVG(speed) OVER (
        PARTITION BY rover_id
        ORDER BY timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_avg_speed

FROM rover_events;
