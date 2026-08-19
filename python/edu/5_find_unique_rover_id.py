# Find unique rover IDs
events = [
    {"rover_id": "R01", "speed": 12},
    {"rover_id": "R02", "speed": 3},
    {"rover_id": "R03", "speed": 18},
    {"rover_id": "R01", "speed": 0},
]

rover_ids = {
    event["rover_id"]
    for event in events
}
