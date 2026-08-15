-- Count events per rover
SELECT
    rover_id,
    COUNT(*) AS event_count
FROM rover_events
GROUP BY rover_id;
