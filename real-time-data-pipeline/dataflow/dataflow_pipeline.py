import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def parse_pubsub_message(message):
    """Parses a Pub/Sub message (bytes) into a dict for BigQuery."""
    try:
        decoded = message.decode("utf-8")
        parsed = json.loads(decoded)
        return parsed
    except Exception as e:
        print(f"Error parsing message: {e}")
        return None


def run():
    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_table', required=True)  # Format: project:dataset.table
    parser.add_argument('--output_path', required=True)   # GCS path
    parser.add_argument('--streaming', action='store_true')
    known_args, pipeline_args = parser.parse_known_args()

    # Set up pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(beam.options.pipeline_options.StandardOptions).streaming = known_args.streaming

    with beam.Pipeline(options=pipeline_options) as p:
        messages = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | "Decode" >> beam.Map(lambda x: x.decode("utf-8"))  # Decode from bytes to string
        )

        # Save raw messages to GCS
        messages | "WriteToGCS" >> beam.io.WriteToText(known_args.output_path + "messages")

        # Parse JSON and filter valid records
        parsed_rows = (
            messages
            | "ParseJSON" >> beam.Map(lambda x: json.loads(x))
            | "FilterValid" >> beam.Filter(lambda record: record is not None)
        )

        # Write to BigQuery
        parsed_rows | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            known_args.output_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location=known_args.output_path + "bq_temp/",
        )


if __name__ == '__main__':
    run()
