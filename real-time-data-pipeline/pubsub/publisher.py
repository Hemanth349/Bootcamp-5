import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows, AfterProcessingTime, AccumulationMode
import json

project_id = "ancient-cortex-465315-t4"
topic_id = "stream-topic"

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
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
                topic=f"projects/{project_id}/topics/{topic_id}",
                with_attributes=False)
            | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
            | "ParseJSON" >> beam.ParDo(ParseMessage())
            # Assign fixed windows of 1 minute with trigger and allowed lateness
            | "WindowIntoFixed" >> beam.WindowInto(
                FixedWindows(60),
                trigger=AfterProcessingTime(10),  # trigger 10 seconds after processing time
                accumulation_mode=AccumulationMode.DISCARDING,
                allowed_lateness=0)
        )

        grouped = (
            messages
            | "KeyByUserId" >> beam.Map(lambda record: (record['user_id'], record))
            | "GroupByUserId" >> beam.GroupByKey()
        )

        # Further processing here

if __name__ == "__main__":
    run()
