import csv
import timeout
import google.cloud import pubsub_v1
import json
from datetime import datetime

# give your gcp project id and pub/sub topic name
project_id=""
topic_id=""

#pub/sub Publisher Client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id,topic_id)

# Function to publish message to pub/sub
def publish_message(emp, firstname, salary):
    timestamp = datetime.utcnow().isoformat() + "Z" # add currnt UTC time
    message = {
        "emp_id": emp_id,
        "firstname": firstname,
        "salary": salary,
        "timestamp":timestamp,
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
        emp_id=row["EmployeeID"]
        firstname=row["FirstName"]
        salary=row["Salary"]
        # Publish the Record
        publish_message(emp_id,firstname,salary)
        # wait for 30 sec before publishing
        time.sleep(30)