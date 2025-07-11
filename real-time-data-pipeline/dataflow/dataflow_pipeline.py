import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


def parse_pubsub_message(message):
    try:
        return json.loads(message)
    except json.JSONDecodeError:
        return {}


def run():
    # Step 1: Let Beam parse all known args first
    pipeline_options = PipelineOptions()
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = True

    # Step 2: Parse your custom arguments separately from sys.argv
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    known_args, _ = parser.parse_known_args()

    # Step 3: Proceed with pipeline
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
