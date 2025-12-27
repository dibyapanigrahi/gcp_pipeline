import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import TimestampedValue
import json
from datetime import datetime

PROJECT_ID = "streaming001"
SUBSCRIPTION = "projects/streaming001/subscriptions/debug-sub"
BQ_TABLE = "streaming001:dataflow.vehicle_events_agg"

class ParseAndTransform(beam.DoFn):
    def process(self, element):
        msg = json.loads(element.decode("utf-8"))

        if msg.get("event_type") == "DROWSY":
            return [(msg["driver_id"], 1)]
        return []

options = PipelineOptions(
    project=PROJECT_ID,
    region="asia-south1",
    temp_location="gs://dataflow-staging-asia-south1-353377002974/streaming_data",
    streaming=True
)
options.view_as(StandardOptions).streaming = True

with beam.Pipeline(options=options) as p:
    (
        p
        | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
        | "Parse" >> beam.ParDo(ParseAndTransform())
        | "Window" >> beam.WindowInto(beam.window.FixedWindows(300))
        | "CountPerDriver" >> beam.CombinePerKey(sum)
        | "FormatForBQ" >> beam.Map(lambda kv: {
            "driver_id": kv[0],
            "drowsy_count": kv[1]
        })
        | "WriteToBQ" >> beam.io.WriteToBigQuery(
            table=BQ_TABLE,
            schema={
                "fields": [
                    {"name": "driver_id", "type": "STRING"},
                    {"name": "drowsy_count", "type": "INTEGER"}
                ]
            },
            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
