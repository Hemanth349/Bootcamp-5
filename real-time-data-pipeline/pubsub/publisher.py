import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
import json

project_id = "ancient-cortex-465315-t4"
topic_id = "stream-topic"
subscription_id = "stream-subscription"  # if you use subscription

class ParseMessage(beam.DoFn):
    def process(self, element):
        record = json.loads(element)
        yield record

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=f"projects/{project_id}/topics/{topic_id}")
            | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
            | "ParseJSON" >> beam.ParDo(ParseMessage())
            # Apply fixed windows of 1 minute
            | "WindowIntoFixed" >> beam.WindowInto(FixedWindows(60))
        )

        grouped = (
            messages
            | "KeyByUserId" >> beam.Map(lambda record: (record['user_id'], record))
            | "GroupByUserId" >> beam.GroupByKey()
        )

        # Example: Write to BigQuery or further processing can be added here
        # grouped | "WriteToBQ" >> beam.io.WriteToBigQuery(...)

if __name__ == "__main__":
    run()
