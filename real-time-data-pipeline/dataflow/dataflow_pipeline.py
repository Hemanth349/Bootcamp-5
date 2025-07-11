import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime

def parse_pubsub_message(message):
    record = json.loads(message)
    record['ingest_time'] = datetime.utcnow().isoformat()
    return record

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--temp_location', required=True)
    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        records = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=known_args.input_topic).with_output_types(bytes)
            | 'Decode' >> beam.Map(lambda b: b.decode('utf-8'))
            | 'ParseJSON' >> beam.Map(parse_pubsub_message)
        )
        
        # Write the parsed records to BigQuery
        records | 'WriteToBigQuery' >> beam.io.WriteToBigQu
