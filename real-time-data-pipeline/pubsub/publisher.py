import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window
import json
import datetime

class ParseMessage(beam.DoFn):
    def process(self, element):
        message = element.decode('utf-8')  # Pub/Sub sends bytes
        timestamp = datetime.datetime.utcnow().isoformat()

        # Send raw message to GCS output
        yield beam.pvalue.TaggedOutput('raw', message)

        # Processed record for BigQuery
        row = {
            'timestamp': timestamp,
            'message': message
        }
        yield row

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

        # Window raw messages before writing to GCS to avoid GroupByKey error
        messages.raw | 'WindowRawMessages' >> beam.WindowInto(window.FixedWindows(60)) \
                     | 'WriteToGCS' >> beam.io.WriteToText(
                         known_args.output_path,
                         file_name_suffix='.txt',
                         shard_name_template='-SS-of-NN'
                     )

if __name__ == '__main__':
    run()
