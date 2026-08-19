events = [
    {"rover_id": "R01", "speed": 10},
    {"rover_id": "R02", "speed": "invalid"},
    {"rover_id": "R03", "speed": 15},
]


speeds = map(
    lambda event: event["speed"],
    events
)


# Option 1
moving = filter(
    lambda event: event["speed"] > 0,
    events
)


# Option 2
moving = [
    event
    for event in events
    if event["speed"] > 0
]
