-- Average battery by model
SELECT
    model,
    AVG(battery_level) AS avg_battery
FROM rovers
GROUP BY model;


-- Raw rovers
--     ↓
-- GROUP BY model
--     ↓
-- one result per model
