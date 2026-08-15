-- Models whose average battery is below 50%
SELECT
    model,
    AVG(battery_level) AS avg_battery
FROM rovers
GROUP BY model
HAVING AVG(battery_level) < 50;


-- WHERE  → filters rows BEFORE aggregation
-- HAVING → filters groups AFTER aggregation
