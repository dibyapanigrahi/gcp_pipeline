from google.cloud import pubsub_v1, bigquery
import json
import time

project_id = "streaming001"
subscription_id = "debug-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

bq_client = bigquery.Client()
table_id = "streaming001.dataflow.tbl_driver_monitoring"

def callback(message):
    data = json.loads(message.data.decode("utf-8"))

    row = [{
        "vehicle_id": data["vehicle_id"],
        "driver_id": data["driver_id"],
        "event_type": data["event_type"],
        "eye_closure_ms": int(data["eye_closure_ms"]),
        "confidence": float(data["confidence"]),
        "event_ts": data["event_ts"]
    }]

    errors = bq_client.insert_rows_json(table_id, row)
    if not errors:
        print("Inserted row")
        message.ack()
    else:
        print("BQ Error:", errors)
        message.nack()

subscriber.subscribe(subscription_path, callback=callback)
print("Listening...")

import time
while True:
    time.sleep(60)
