-- Show rover events with rover information
SELECT
    e.event_id,
    e.rover_id,
    r.model,
    r.status,
    e.timestamp,
    e.speed
FROM rover_events e
JOIN rovers r
    ON e.rover_id = r.rover_id;


-- rover_events
--       │
--       │ rover_id
--       ▼
--     rovers


-- Find observations where the rover exceeded the road's speed limit
SELECT
    t.rover_id,
    t.timestamp,
    t.road_id,
    t.speed,
    r.max_speed
FROM trajectory_edges t
JOIN roads r
    ON t.road_id = r.road_id
WHERE t.speed > r.max_speed;
