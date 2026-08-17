# Build an ETL pipeline
events = [ 
    {
        "rover_id": "R01",
        "sensors": [
            {"name": "temperature", "value": 42.5},
            {"name": "motor_current", "value": 3.2},
            {"name": "battery", "value": 91}
        ]
    },
    {
        "rover_id": "R02",
        "sensors": [
            {"name": "temperature", "value": 35.7},
            {"name": "motor_current", "value": 2.9},
            {"name": "battery", "value": 75}
        ]
    }
]


def extract_sensor(event, name):
    for sensor in event.get("sensors", []):
        if sensor["name"] == name:
            return sensor["value"]
    return None


def create_features(event):
    location = event.get("location") or {}

    return {
        "rover_id": event["rover_id"],
        "timestamp": event["timestamp"],
        "speed": event.get("speed", 0.0),
        "battery": extract_sensor(event, "battery"),
        "temperature": extract_sensor(event, "temperature"),
        "latitude": location.get("latitude"),
        "longitude": location.get("longitude")
    }


def filter_moving(events):
    for event in events:
        if event["speed"] > 0:
            yield event


def add_features(events):
    for event in events:
        yield create_features(event)


def validate(events):
    for event in events:
        if event["battery"] is not None:
            yield event


pipeline = filter_moving(events)
pipeline = add_features(pipeline)
pipeline = validate(pipeline)

for feature in pipeline:
    print(feature)


# Raw events
#     ↓
# Filter
#     ↓
# Feature calculation
#     ↓
# Validation
#     ↓
# Output
