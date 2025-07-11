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

    # Add required options explicitly to pipeline_args:
    pipeline_args.extend([
        f'--project={known_args.project}',
        f'--region={known_args.region}',
        f'--temp_location={known_args.temp_location}',
        f'--staging_location={known_args.temp_location}/staging',
        '--runner=DataflowRunner',
        '--streaming'
    ])

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
