from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from google.cloud import bigquery
from datetime import datetime, timezone
import json
import time

print("Starting Kafka → BigQuery consumer...")

# Wait for Kafka to be ready
while True:
    try:
        consumer = KafkaConsumer(
            "clickstream",
            bootstrap_servers="kafka:9092",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="bq-consumer"
        )
        break
    except NoBrokersAvailable:
        print("Kafka not ready, retrying in 5 seconds...")
        time.sleep(5)

print("Connected to Kafka")

client = bigquery.Client.from_service_account_json("/app/gcp_key.json")
table_id = "streaming001.kafka_demo.click_events"

print("Waiting for messages...")

for message in consumer:
    row = message.value
    print("Received:", row)

    # Normalize timestamp for BigQuery
    try:
        row["event_time"] = (
            datetime.fromisoformat(row["event_time"])
            .astimezone(timezone.utc)
            .isoformat()
        )
    except Exception as e:
        print("Invalid timestamp, skipping:", row, e)
        continue

    errors = client.insert_rows_json(table_id, [row])

    if errors:
        print("BigQuery Insert Errors:", errors)
    else:
        print("Inserted into BigQuery:", row)

    time.sleep(1)
