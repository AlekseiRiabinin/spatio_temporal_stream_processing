-- Calculate average speed per rover and then select rovers whose average speed exceeds 15
WITH rover_speed AS (
    SELECT
        rover_id,
        AVG(speed) AS avg_speed
    FROM rover_events
    GROUP BY rover_id
)
SELECT
    rover_id,
    avg_speed
FROM rover_speed
WHERE avg_speed > 15;
