-- Show all rovers, even those without events
SELECT
    r.rover_id,
    r.status,
    e.timestamp
FROM rovers r
LEFT JOIN rover_events e
    ON r.rover_id = e.rover_id;

