import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

def run():
    # Use argparse to get custom arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--streaming', action='store_true')

    # Parse known and unknown args separately
    known_args, pipeline_args = parser.parse_known_args()

    # Setup pipeline options from CLI args
    pipeline_options = PipelineOptions(pipeline_args)

    # Enable streaming if specified
    pipeline_options.view_as(StandardOptions).streaming = known_args.streaming

    # Start the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # Your actual logic here
        (p
         | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
         | "Write to GCS" >> beam.io.WriteToText(known_args.output_path + '/output')
        )

if __name__ == '__main__':
    run()
