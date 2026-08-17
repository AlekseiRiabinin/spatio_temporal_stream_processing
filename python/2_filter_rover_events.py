# Find moving rovers
events = [
    {"rover_id": "R01", "speed": 12},
    {"rover_id": "R02", "speed": 3},
    {"rover_id": "R03", "speed": 18},
    {"rover_id": "R01", "speed": 0},
]

# Option 1
moving = [
    event
    for event in events
    if event["speed"] > 0
]

# Option 2
moving = list(filter(
    lambda event: event["speed"] > 0,
    events
))
