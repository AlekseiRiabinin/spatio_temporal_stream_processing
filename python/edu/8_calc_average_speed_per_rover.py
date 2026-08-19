# Calculate average speed per rover
events = [
    {"rover_id": "R01", "speed": 10},
    {"rover_id": "R02", "speed": 20},
    {"rover_id": "R01", "speed": 15},
    {"rover_id": "R03", "speed": 5},
    {"rover_id": "R02", "speed": 18},
]

from collections import defaultdict

totals = defaultdict(float)
counts = defaultdict(int)

for event in events:
    rover_id = event["rover_id"]

    totals[rover_id] += event["speed"]
    counts[rover_id] += 1

averages = {
    rover_id: totals[rover_id] / counts[rover_id]
    for rover_id in totals
}
