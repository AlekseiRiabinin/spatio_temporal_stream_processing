# Count how many events each rover generated
events = [
    {"rover_id": "R01", "speed": 10},
    {"rover_id": "R02", "speed": 20},
    {"rover_id": "R01", "speed": 15},
    {"rover_id": "R03", "speed": 5},
    {"rover_id": "R02", "speed": 18},
]

# Option 1
counts = {}

for event in events:
    rover_id = event["rover_id"]

    counts[rover_id] = counts.get(rover_id, 0) + 1

print(counts)

# Option 2
from collections import defaultdict

counts = defaultdict(int)

for event in events:
    counts[event["rover_id"]] += 1
