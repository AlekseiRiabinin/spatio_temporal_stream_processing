# Extract the rover's latitude and longitude
event = {
    "rover_id": "R01",
    "location": {
        "latitude": 55.7558,
        "longitude": 37.6173
    }
}

latitude = event["location"]["latitude"]
longitude = event["location"]["longitude"]

print(latitude, longitude)


# A safer approach
location = event.get("location") or {}

latitude = location.get("latitude")
longitude = location.get("longitude")
