import json
import time
import random
from kafka import KafkaProducer


def main():
    producer = KafkaProducer(
        bootstrap_servers="kafka-1:19092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    while True:
        msg = {
            "id": f"user-{random.randint(1, 5)}",
            "ts": int(time.time() * 1000),
            "value": round(random.uniform(5.0, 15.0), 2)
        }

        producer.send("streamio-test-topic", msg)
        print("Sent:", msg)
        time.sleep(0.2)


if __name__ == "__main__":
    main()
