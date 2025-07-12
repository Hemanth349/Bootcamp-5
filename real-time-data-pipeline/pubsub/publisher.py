import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import datetime

def run():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--runner', required=True)
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)

    known_args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = known_args.runner
    options.view_as(StandardOptions).project = known_args.project
    options.view_as(StandardOptions).region = known_args.region
    options.view_as(StandardOptions).temp_location = known_args.temp_location
    options.view_as(StandardOptions).staging_location = known_args.staging_location

    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | 'ParseMessages' >> beam.ParDo(ParseMessage()).with_outputs('raw', main='bq')
        )

        # Write processed messages to BigQuery
        messages.bq | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            known_args.output_table,
            schema='timestamp:TIMESTAMP,message:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Window raw messages with triggers to avoid GroupByKey error on streaming
        messages.raw | 'WindowRawMessages' >> beam.WindowInto(
            window.FixedWindows(60),
            allowed_lateness=0,
            trigger=window.AfterProcessingTime(60),
            accumulation_mode=window.AccumulationMode.DISCARDING
        ) | 'WriteToGCS' >> beam.io.WriteToText(
            known_args.output_path,
            file_name_suffix='.txt',
            shard_name_template='-SS-of-NN'
        )

if __name__ == '__main__':
    run()
