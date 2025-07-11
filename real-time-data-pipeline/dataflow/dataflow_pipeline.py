import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import sys
import json

def parse_pubsub_message(message):
    # Example parse logic
    return json.loads(message)

def run():
    # Use all CLI args directly, including --project and --region
    pipeline_options = PipelineOptions(sys.argv[1:])
    pipeline_options.view_as(StandardOptions).streaming = True

    # Parse known args for your custom parameters
    parser = pipeline_options.get_all_options()
    input_topic = parser.get('input_topic')
    output_table = parser.get('output_table')
    output_path = parser.get('output_path')

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
