from kafka import KafkaProducer
import json, time, random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

pages = ["/home", "/product", "/cart", "/checkout"]

while True:
    event = {
        "user_id": f"user_{random.randint(1,100)}",
        "page": random.choice(pages),
        "event_time": datetime.utcnow().isoformat()
    }
    producer.send("clickstream", event)
    print("Sent:", event)
    time.sleep(2)
