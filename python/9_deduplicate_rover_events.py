# Remove duplicate events
events = [
    {"event_id": 1, "rover_id": "R01", "speed": 10},
    {"event_id": 2, "rover_id": "R01", "speed": 15},
    {"event_id": 1, "rover_id": "R01", "speed": 10},
]

seen = set()
unique_events = []

for event in events:
    event_id = event["event_id"]

    if event_id not in seen:
        seen.add(event_id)
        unique_events.append(event)


# Deduplicate while keeping the latest event
events = [
    {"event_id": 1, "timestamp": 100, "speed": 10},
    {"event_id": 1, "timestamp": 105, "speed": 12},
    {"event_id": 2, "timestamp": 110, "speed": 20},
]

latest = {}

for event in events:
    event_id = event["event_id"]

    if (
        event_id not in latest
        or event["timestamp"] > latest[event_id]["timestamp"]
    ):
        latest[event_id] = event

result = list(latest.values())
