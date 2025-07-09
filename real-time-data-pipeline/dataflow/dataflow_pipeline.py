import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
from apache_beam.io.gcp.bigquery import WriteToBigQuery

class ParseJSON(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode('utf-8'))
        yield record

def run():
    options = PipelineOptions(
        project='your-project-id',
        region='us-central1',
        temp_location='gs://your-temp-bucket/temp',
        streaming=True
    )
    with beam.Pipeline(options=options) as p:
        raw_messages = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic="projects/your-project-id/topics/stream-topic")
            | "ParseJSON" >> beam.ParDo(ParseJSON())
        )

        # Write processed data to BigQuery
        raw_messages | "WriteToBigQuery" >> WriteToBigQuery(
            table='your-project-id:streaming_data.processed_events',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        # Write raw messages to Cloud Storage (as backup)
        (
            p
            | "ReadRawMessages" >> beam.io.ReadFromPubSub(topic="projects/your-project-id/topics/stream-topic")
            | "WriteToGCS" >> beam.io.WriteToText("gs://your-raw-data-bucket/raw/messages", file_name_suffix=".json")
        )

if __name__ == "__main__":
    run()
