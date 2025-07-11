import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
import sys
import json

def parse_pubsub_message(message):
    return json.loads(message)

def run():
    # Read CLI args
    pipeline_options = PipelineOptions(sys.argv[1:])
    
    # Explicitly set GoogleCloudOptions for project and region to avoid validation errors
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    standard_options = pipeline_options.view_as(StandardOptions)

    # Make sure project and region are set explicitly (these come from CLI args)
    if not google_cloud_options.project:
        raise ValueError('Missing required pipeline option: --project')
    if not google_cloud_options.region:
        raise ValueError('Missing required pipeline option: --region')

    # Enable streaming mode
    standard_options.streaming = True

    # Access custom options:
    opts = pipeline_options.get_all_options()
    input_topic = opts.get('input_topic')
    output_table = opts.get('output_table')
    output_path = opts.get('output_path')

    with beam.Pipeline(options=pipeline_options) as p:
        records = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=input_topic).with_output_types(bytes)
            | 'Decode' >> beam.Map(lambda b: b.decode('utf-8'))
            | 'ParseJSON' >> beam.Map(parse_pubsub_message)
        )
        records | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            output_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
        records | 'ArchiveToGCS' >> beam.io.WriteToText(
            output_path,
            file_name_suffix=".json",
            shard_name_template="-SS-of-NN"
        )

if __name__ == '__main__':
    run()
