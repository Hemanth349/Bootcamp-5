import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

def run():
    # Parse command-line arguments using argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--streaming', action='store_true')
    known_args, pipeline_args = parser.parse_known_args()

    # Correctly construct PipelineOptions with CLI args
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(beam.options.pipeline_options.StandardOptions).streaming = known_args.streaming

    # Now Beam can detect project, region, temp_location, etc.
    with beam.Pipeline(options=pipeline_options) as p:
        # Example pipeline
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | "WriteToGCS" >> beam.io.WriteToText(known_args.output_path + "output")
        )

if __name__ == '__main__':
    run()
