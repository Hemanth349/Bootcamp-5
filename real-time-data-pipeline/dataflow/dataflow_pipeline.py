import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

def run(argv=None):
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--runner', required=True)
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    args, pipeline_args = parser.parse_known_args(argv)

    # Set pipeline options
    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        # Read from Pub/Sub topic
        records = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=args.input_topic)
            | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
        )

        # Apply windowing before GroupByKey
        windowed_records = (
            records
            | 'WindowIntoFixed' >> beam.WindowInto(FixedWindows(60))  # 60 second windows
        )

        # Assuming your records are key-value pairs, for example:
        # Transform records into key-value tuples before GroupByKey
        # Example: (user_id, action)
        kv_pairs = (
            windowed_records
            | 'ParseToKV' >> beam.Map(lambda record: parse_record_to_kv(record))
        )

        grouped = kv_pairs | 'GroupByKey' >> beam.GroupByKey()

        # Process grouped data here
        # For example, count actions per user in each window
        results = (
            grouped
            | 'CountActions' >> beam.Map(lambda kv: (kv[0], len(kv[1])))
        )

        # Write results to BigQuery (example)
        results | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            args.output_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Archive raw data to GCS as text files
        records | 'ArchiveToGCS' >> beam.io.WriteToText(
            file_path_prefix=args.output_path,
            file_name_suffix='.txt'
        )

def parse_record_to_kv(record):
    # Example parse function; modify to your schema
    # Suppose record is a JSON string like '{"user_id":"123", "action":"click"}'
    import json
    data = json.loads(record)
    return data['user_id'], data['action']

if __name__ == '__main__':
    run()
