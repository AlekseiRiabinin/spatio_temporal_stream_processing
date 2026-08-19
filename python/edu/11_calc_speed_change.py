# Calculate speed change
events = [
    {"timestamp": 100, "speed": 10},
    {"timestamp": 110, "speed": 15},
    {"timestamp": 120, "speed": 12},
]


previous_speed = None

for event in events:
    speed = event["speed"]

    if previous_speed is None:
        event["speed_change"] = None
    else:
        event["speed_change"] = speed - previous_speed

    previous_speed = speed
