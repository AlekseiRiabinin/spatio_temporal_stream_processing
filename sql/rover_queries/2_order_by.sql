-- Show rovers with lowest battery first
SELECT
    rover_id,
    battery_level
FROM rovers
ORDER BY battery_level ASC;
