import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from apache_beam.transforms.window import FixedWindows
import argparse
import json

def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--project',
        required=True,
        help='Google Cloud project ID')
    parser.add_argument(
        '--region',
        required=True,
        help='GCP region')
    parser.add_argument(
        '--runner',
        required=True,
        help='Pipeline runner (DataflowRunner or DirectRunner)')
    parser.add_argument(
        '--input_topic',
        required=True,
        help='Input Pub/Sub topic of the form "projects/{project_id}/topics/{topic_name}"')
    parser.add_argument(
        '--output_table',
        required=True,
        help='BigQuery output table of the form "project:dataset.table"')
    parser.add_argument(
        '--output_path',
        required=True,
        help='GCS path for archiving raw data, e.g. gs://bucket/path')
    parser.add_argument(
        '--temp_location',
        required=True,
        help='GCS temp location for Dataflow')
    parser.add_argument(
        '--staging_location',
        required=True,
        help='GCS staging location for Dataflow')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).runner = known_args.runner
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Define a simple parsing function to parse JSON messages from Pub/Sub
    def parse_pubsub_message(message):
        try:
            decoded = message.decode('utf-8')
            return json.loads(decoded)
        except Exception as e:
            # In case of malformed JSON, skip or log as needed
            return None

    # Transform parsed record into a BigQuery row and also create a key-value for grouping (example)
    def to_bq_row(record):
        # Example: assuming record has user_id and action fields
        return {
            'user_id': record.get('user_id', ''),
            'action': record.get('action', ''),
            'timestamp': record.get('timestamp', '')
        }

    # Create a KV pair for GroupByKey (example key: user_id)
    def to_kv(record):
        return (record.get('user_id', 'unknown'), record)

    with beam.Pipeline(options=pipeline_options) as p:

        raw_records = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic, with_attributes=False)
            | "ParseJSON" >> beam.Map(parse_pubsub_message)
            | "FilterValid" >> beam.Filter(lambda x: x is not None)
        )

        # Archive raw records as JSON lines to GCS
        (
            raw_records
            | "ConvertToJsonStr" >> beam.Map(json.dumps)
            | "ArchiveToGCS" >> beam.io.WriteToText(
                known_args.output_path + '/raw_records',
                file_name_suffix='.json',
                shard_name_template='-SS-of-NN',
                num_shards=5
            )
        )

        # Window and GroupByKey example: windowed by 1 minute fixed windows
        windowed_kv_records = (
            raw_records
            | "ToKV" >> beam.Map(to_kv)
            | "ApplyWindow" >> beam.WindowInto(FixedWindows(60))
            | "GroupByUserId" >> beam.GroupByKey()
        )

        # Flatten grouped records (example: just take first action per user per window)
        flattened_records = (
            windowed_kv_records
            | "ExtractFirstRecord" >> beam.Map(lambda kv: to_bq_row(kv[1][0]) if kv[1] else None)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        # Write to BigQuery
        flattened_records | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            known_args.output_table,
            schema='user_id:STRING, action:STRING, timestamp:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    run()
