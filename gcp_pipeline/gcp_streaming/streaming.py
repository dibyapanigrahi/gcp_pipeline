import csv
import time
from google.cloud import pubsub_v1
import json
from datetime import datetime

# give your gcp project id and pub/sub topic name
project_id="streaming001"
topic_id="df-vehicle-data"
csv_file_path="driver_data.csv"
#pub/sub Publisher Client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id,topic_id)

# Function to publish message to pub/sub
def publish_message(vehicle_id, driver_id, event_type, eye_closure_ms, confidence):
    timestamp = datetime.utcnow().isoformat() + "Z" # add currnt UTC time
    message = {
        "vehicle_id": vehicle_id,
        "driver_id": driver_id,
        "event_type": event_type,
        "eye_closure_ms": eye_closure_ms,
        "confidence": confidence,
        "event_ts": timestamp,
    }

    # Convert message to JSON and encode as bytes
    message_json = json.dumps(message).encode("utf-8")
    #Publish the message
    publish_status = publisher.publish(topic_path, message_json)
    print(f"Published message with ID: {publish_status.result()}")

# PAth to your csv file
with open(csv_file_path, mode="r") as file:
    reader=csv.DictReader(file)
    for row in reader:
        vehicle_id=row["vehicle_id"]
        driver_id=row["driver_id"]
        event_type=row["event_type"]
        eye_closure_ms=row["eye_closure_ms"]
        confidence=row["confidence"]
        # Publish the Record
        publish_message(vehicle_id,driver_id,event_type,eye_closure_ms,confidence)
        # wait for 30 sec before publishing
        time.sleep(2)