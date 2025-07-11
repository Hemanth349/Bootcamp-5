import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

class ParseJsonDoFn(beam.DoFn):
    def process(self, element):
        import json
        record = json.loads(element)
        # Return as a list of dict (BigQuery row format)
        yield record

def run():
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
    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(PipelineOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        records = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic).with_output_types(bytes)
            | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
            | "ParseJson" >> beam.ParDo(ParseJsonDoFn())
        )

        # Write to BigQuery
        records | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            known_args.output_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method='STREAMING_INSERTS'
        )

        # Archive raw JSON to GCS (append newline for each record)
        records | "FormatJsonForGCS" >> beam.Map(lambda row: str(row).replace("'", '"')) \
                | "WriteToGCS" >> beam.io.WriteToText(
                    known_args.output_path + '/archive/data',
                    file_name_suffix='.json',
                    shard_name_template='-SS-of-NN'
                )

if __name__ == '__main__':
    run()
