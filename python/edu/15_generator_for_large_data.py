# Generator for large rover data
import random
from datetime import datetime, timedelta

def generate_events(n=1_000_000):
    base_time = datetime(2026, 8, 15, 10, 30, 45)

    for i in range(n):
        ts = base_time + timedelta(seconds=i)

        yield {
            "rover_id": f"R{i:03d}",
            "timestamp": ts.isoformat() + "Z",
            "sensors": [
                {"name": "temperature", "value": 35 + (i % 15)},
                {"name": "battery", "value": 100 - (i % 40)}
            ],
            "speed": round(random.uniform(0.0, 2.5), 2),
            "location": {
                "latitude": 25.2048 + random.uniform(-0.0005, 0.0005),
                "longitude": 55.2708 + random.uniform(-0.0005, 0.0005)
            }
        }


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


def send_to_model(feature):
    print(f"Sending to model: {feature}")


def generate_features():
    for event in generate_events():
        yield create_features(event)


# Stream features directly to the model
for feature in generate_features():
    send_to_model(feature)



# list comprehension
#     ↓
# materializes everything

# generator
#     ↓
# processes lazily
