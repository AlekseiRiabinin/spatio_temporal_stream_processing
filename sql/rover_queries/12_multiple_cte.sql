-- Find active rovers whose average speed is above the average speed of all active rovers
WITH rover_avg AS (
    SELECT
        rover_id,
        AVG(speed) AS avg_speed
    FROM rover_events
    GROUP BY rover_id
),
active_rovers AS (
    SELECT
        rover_id
    FROM rovers
    WHERE status = 'active'
),
active_avg AS (
    SELECT
        AVG(r.avg_speed) AS global_avg_speed
    FROM rover_avg r
    JOIN active_rovers a
        ON r.rover_id = a.rover_id
)
SELECT
    r.rover_id,
    r.avg_speed
FROM rover_avg r
JOIN active_rovers a
    ON r.rover_id = a.rover_id
CROSS JOIN active_avg g
WHERE r.avg_speed > g.global_avg_speed;
