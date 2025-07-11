import argparse
import json
import sys
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


def parse_pubsub_message(message):
    """Parse JSON string from Pub/Sub into a Python dictionary."""
    try:
        return json.loads(message)
    except json.JSONDecodeError:
        return {}


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    
    # Only used for internal logic, full args go to Beam
    known_args, _ = parser.parse_known_args()

    # Pass full args to Beam
    pipeline_options = PipelineOptions(sys.argv[1:])
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        records = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=known_args.input_topic).with_output_types(bytes)
            | 'Decode' >> beam.Map(lambda b: b.decode('utf-8'))
            | 'ParseJSON' >> beam.Map(parse_pubsub_message)
        )

        # Write to BigQuery
        records | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            known_args.output_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Archive to GCS
        records | 'ArchiveToGCS' >> beam.io.WriteToText(
            known_args.output_path,
            file_name_suffix=".json",
            shard_name_template="-SS-of-NN"
        )


if __name__ == '__main__':
    run()
