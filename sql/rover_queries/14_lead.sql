-- Show the next timestamp for each rover (time spent on road segment)
SELECT
    rover_id,
    timestamp,

    LEAD(timestamp) OVER (
        PARTITION BY rover_id
        ORDER BY timestamp
    ) AS next_timestamp

FROM rover_events;
