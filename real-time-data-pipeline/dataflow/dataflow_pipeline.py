import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
import argparse
import sys

def run():
    # Parse arguments from CLI
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--streaming', action='store_true')

    # Required for Beam to recognize core options like --project, --region, etc.
    known_args, pipeline_args = parser.parse_known_args()

    # Beam pipeline options
    options = PipelineOptions(pipeline_args)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = options.view_as(GoogleCloudOptions).project
    google_cloud_options.region = options.view_as(GoogleCloudOptions).region
    google_cloud_options.staging_location = options.view_as(GoogleCloudOptions).staging_location
    google_cloud_options.temp_location = options.view_as(GoogleCloudOptions).temp_location

    options.view_as(StandardOptions).streaming = known_args.streaming

    with beam.Pipeline(options=options) as p:
        # Replace with your actual pipeline logic
        (p
         | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
         | "Write to GCS" >> beam.io.WriteToText(known_args.output_path + 'output')
        )
