-- Show active rovers"
SELECT
    rover_id,
    model,
    battery_level
FROM rovers
WHERE status = 'active';
