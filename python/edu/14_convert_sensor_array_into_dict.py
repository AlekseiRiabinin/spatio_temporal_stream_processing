# Convert sensor array into a dictionary
sensors = [
    {"name": "temperature", "value": 42.5},
    {"name": "battery", "value": 91},
    {"name": "motor_current", "value": 3.2}
]

# convert to: 
{
    "temperature": 42.5,
    "battery": 91,
    "motor_current": 3.2
}


sensor_map = {
    sensor["name"]: sensor["value"]
    for sensor in sensors
}

battery = sensor_map.get("battery")
