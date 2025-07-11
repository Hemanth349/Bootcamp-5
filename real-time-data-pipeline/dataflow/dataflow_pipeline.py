import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import sys
import json

def parse_pubsub_message(message):
    return json.loads(message)

def run():
    # Pass all CLI args directly
    pipeline_options = PipelineOptions(sys.argv[1:])
    pipeline_options.view_as(StandardOptions).streaming = True

    # Access your custom args from the pipeline options
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
