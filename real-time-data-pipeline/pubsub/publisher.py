import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
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
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--bq_table', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--output_table', required=True)


    known_args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Set Google Cloud specific options explicitly (optional but good practice)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = known_args.project
    google_cloud_options.region = known_args.region
    google_cloud_options.temp_location = known_args.temp_location
    google_cloud_options.staging_location = known_args.staging_location

    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | 'ParseMessages' >> beam.ParDo(ParseMessage()).with_outputs('raw', main='bq')
        )

        # Write to BigQuery
        messages.bq | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            known_args.bq_table,
            schema='timestamp:TIMESTAMP,message:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Write raw to GCS
        messages.raw | 'WriteToGCS' >> beam.io.WriteToText(
            known_args.output_path,
            file_name_suffix='.txt',
            shard_name_template='-SS-of-NN'
        )

if __name__ == '__main__':
    run()
