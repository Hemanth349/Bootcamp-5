import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

def parse_pubsub_message(message):
    try:
        return json.loads(message)
    except json.JSONDecodeError:
        return {'error': 'invalid_json'}

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--output_path', required=True)
    
    # Do not add project/region manually — let PipelineOptions handle it
    known_args, pipeline_args = parser.parse_known_args(argv)

    # ✅ This MUST be passed to PipelineOptions
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        records = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=known_args.input_topic).with_output_types(bytes)
            | 'Decode' >> beam.Map(lambda b: b.decode('utf-8'))
            | 'ParseJSON' >> beam.Map(parse_pubsub_message)
        )

        records | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            known_args.output_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        records | 'ArchiveToGCS' >> beam.io.WriteToText(
            known_args.output_path,
            file_name_suffix=".json",
            shard_name_template="-SS-of-NN"
        )

if __name__ == '__main__':
    import sys
    run(sys.argv)
