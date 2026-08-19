# Exception handling for bad telemetry
events = [
    {"rover_id": "R01", "speed": 10},
    {"rover_id": "R02", "speed": "invalid"},
    {"rover_id": "R03", "speed": 15},
]


def send_to_feature_store(result):
    ...


def process_event(event):
    try:
        speed = float(event["speed"])

        return {
            "rover_id": event["rover_id"],
            "speed": speed
        }

    except (KeyError, ValueError) as exc:
        print(f"Invalid event: {event}: {exc}")
        return None


for event in events:
    result = process_event(event)

    if result is not None:
        send_to_feature_store(result)
