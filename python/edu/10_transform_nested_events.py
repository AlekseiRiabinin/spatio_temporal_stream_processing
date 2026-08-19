# Transform nested events
events = [
    {
        "rover_id": "Rover-2024-001",
        "timestamp": "2026-08-15T10:30:45.123Z",
        "speed": 2.5,
        "battery": 87.3,
        "location": {
            "latitude": 45.123456,
            "longitude": -75.987654,
            "altitude": 124.5,
            "accuracy": 2.1
        },
        "temperature": -15.2,
        "wheel_angle": 0.0,
        "mission_phase": "navigation"
    },
    {
        "rover_id": "Rover-2024-001",
        "timestamp": "2026-08-15T10:31:23.789Z",
        "speed": 3.2,
        "battery": 85.6,
        "location": {
            "latitude": 45.123789,
            "longitude": -75.987321,
            "altitude": 123.1,
            "accuracy": 2.3
        },
        "temperature": -14.8,
        "wheel_angle": -2.5,
        "mission_phase": "navigation"
    }
]


# Option 1
def create_features(event):
    location = event.get("location") or {}

    return {
        "rover_id": event["rover_id"],
        "timestamp": event["timestamp"],
        "speed": event["speed"],
        "battery": event["battery"],
        "latitude": location.get("latitude"),
        "longitude": location.get("longitude"),
    }

features = [
    create_features(event)
    for event in events
]


# Option 2
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class Location:
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude: Optional[float] = None
    accuracy: Optional[float] = None


@dataclass
class RoverFeatures:
    rover_id: str
    timestamp: str
    speed: float
    battery: float
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    @classmethod
    def from_event(cls, event: dict) -> "RoverFeatures":
        location = event.get("location") or {}
        return cls(
            rover_id=event["rover_id"],
            timestamp=event["timestamp"],
            speed=event["speed"],
            battery=event["battery"],
            latitude=location.get("latitude"),
            longitude=location.get("longitude"),
        )

features_objs = [RoverFeatures.from_event(e) for e in events]
features = [asdict(f) for f in features_objs]

print(features_objs[0])
print(features[0])
