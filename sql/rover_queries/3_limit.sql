-- Show the 5 rovers with the lowest battery
SELECT
    rover_id,
    battery_level
FROM rovers
ORDER BY battery_level
LIMIT 5;
