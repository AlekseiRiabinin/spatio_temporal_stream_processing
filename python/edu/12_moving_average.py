# Calculate average speed over last 3 observations
events = [
    {"timestamp": 100, "speed": 10},
    {"timestamp": 110, "speed": 15},
    {"timestamp": 120, "speed": 12},
]


from collections import deque

speeds = deque(maxlen=3)

for event in events:
    speeds.append(event["speed"])

    event["avg_speed_3"] = sum(speeds) / len(speeds)

