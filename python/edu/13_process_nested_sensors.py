# Extract the battery sensor value
event = {
    "rover_id": "R01",
    "sensors": [
        {"name": "temperature", "value": 42.5},
        {"name": "motor_current", "value": 3.2},
        {"name": "battery", "value": 91}
    ]
}


battery = next(
    (
        sensor["value"]
        for sensor in event.get("sensors", [])
        if sensor["name"] == "battery"
    ),
    None
)
