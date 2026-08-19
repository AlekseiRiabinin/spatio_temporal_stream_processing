# Group events by rover
events = [
    {"rover_id": "R01", "speed": 10},
    {"rover_id": "R02", "speed": 20},
    {"rover_id": "R01", "speed": 15},
    {"rover_id": "R03", "speed": 5},
    {"rover_id": "R02", "speed": 18},
]

from collections import defaultdict

events_by_rover = defaultdict(list)

for event in events:
    events_by_rover[event["rover_id"]].append(event)
