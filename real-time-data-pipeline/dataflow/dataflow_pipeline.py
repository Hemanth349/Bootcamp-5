import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
import json
import sys

class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_topic', required=True, help='Input Pub/Sub topic')
        parser.add_argument('--output_table', required=True, help='Output BigQuery table <project>:<dataset>.<table>')
        parser.add_argument('--output_path', required=True, help='Output GCS path for raw data')

def parse_pubsub_message(message):
    return json.loads(message)

def run(argv=None):
    if argv is None:
        argv = sys.argv

    pipeline_options = PipelineOptions(argv)
    custom_options = pipeline_options.view_as(CustomOptions)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    standard_options = pipeline_options.view_as(StandardOptions)
    setup_options = pipeline_options.view_as(SetupOptions)

    # Validate required options
    if not google_cloud_options.project:
        raise ValueError("Missing required option --project")
    if not google_cloud_options.region:
        raise ValueError("Missing required option --region")

    # Enable streaming mode if flag is present
    standard_options.streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Read messages from Pub/Sub
        messages = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=custom_options.input_topic).with_output_types(bytes)
            | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
            | 'ParseJSON' >> beam.Map(parse_pubsub_message)
        )

        # Write to BigQuery
        messages | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            custom_options.output_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Also write raw JSON to GCS for backup/archive
        (
            p
            | 'ReadFromPubSubForBackup' >> beam.io.ReadFromPubSub(topic=custom_options.input_topic).with_output_types(bytes)
            | 'DecodeBackup' >> beam.Map(lambda x: x.decode('utf-8'))
            | 'WriteToGCS' >> beam.io.WriteToText(
                custom_options.output_path,
                file_name_suffix='.json',
                shard_name_template='-SS-of-NN'
            )
        )

if __name__ == '__main__':
    run()
