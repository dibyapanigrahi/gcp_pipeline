from google.cloud import pubsub_v1
import json

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("adas-project", "driver_monitoring")

event = {
    "vehicle_id": "KA01AB1234",
    "event_type": "eye_closed",
    "duration_ms": 1200
}

publisher.publish(topic_path, json.dumps(event).encode("utf-8"))
