# Sort events by speed
events = [
    {"rover_id": "R01", "speed": 12},
    {"rover_id": "R02", "speed": 3},
    {"rover_id": "R03", "speed": 18},
    {"rover_id": "R01", "speed": 0},
]

sorted_events = sorted(
    events,
    key=lambda event: event["speed"],
    reverse=True
)
