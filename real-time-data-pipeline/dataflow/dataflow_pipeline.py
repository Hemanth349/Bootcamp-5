import sys
print("Received args:", sys.argv)
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import json


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input_topic',
            required=True,
            help='Input Pub/Sub topic of the form projects/{project}/topics/{topic}')
        parser.add_argument(
            '--output_table',
            required=True,
            help='Output BigQuery table for results specified as: PROJECT:DATASET.TABLE')
        parser.add_argument(
            '--output_path',
            required=True,
            help='GCS path for output files, e.g. gs://bucket/path')

def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    custom_options = pipeline_options.view_as(CustomOptions)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    standard_options = pipeline_options.view_as(StandardOptions)

    standard_options.streaming = True  # Enable streaming mode

    if not google_cloud_options.project:
        raise ValueError("Missing required option --project")
    if not google_cloud_options.region:
        raise ValueError("Missing required option --region")

    with beam.Pipeline(options=pipeline_options) as p:
        # Read messages from Pub/Sub topic (as bytes)
        raw_messages = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=custom_options.input_topic)

        # Decode bytes to string
        decoded = raw_messages | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))

        # Parse JSON messages
        parsed = decoded | "ParseJSON" >> beam.Map(json.loads)

        # Write parsed messages to BigQuery
        parsed | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            custom_options.output_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Optional: Write raw decoded messages to GCS for backup/archive
        decoded | "WriteToGCS" >> beam.io.WriteToText(
            custom_options.output_path,
            file_name_suffix=".json",
            shard_name_template="-SS-of-NN"
        )

if __name__ == '__main__':
    run()
