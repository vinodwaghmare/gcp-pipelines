import argparse
from datetime import datetime
import logging
import random
import apache_beam as beam
from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


class GroupMessagesByFixedWindows(PTransform):
    """A composite transform that groups Pub/Sub messages based on publish time and outputs a list of tuples, each containing a message and its publish time."""
    def __init__(self, window_size, num_shards=5):
          # Set window size to 60 seconds.
          self.window_size = int(window_size * 60)
          self.num_shards = num_shards
    def expand(self, pcoll):
          return (
              pcoll
              # Bind window info to each element using element timestamp (or publish time).
              | "Window into fixed intervals"
              >> WindowInto(FixedWindows(self.window_size))
              | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
              # Assign a random key to each windowed element based on the number of shards.
              | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
              # Group windowed elements by key. All the elements in the same window must fit
              # memory for this. If not, you need to use `beam.util.BatchElements`.
              | "Group by key" >> GroupByKey()
          )
class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
    publish time into a tuple.
    """
        yield (
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
            ),
            )

# parsing method to prepare data to write into BigQuery
def parse_method(string_input):
    row = {'batch_num': string_input[0], 'message':string_input[1][0][0], 'send_time':string_input[1][0][1]}
    return row

def run(input_topic, output_path, window_size=1.0, num_shards=5, pipeline_args=None): # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
    pipeline_args, streaming=True, save_main_session=True
    )
    with Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
              # Because `timestamp_attribute` is unspecified in `ReadFromPubSub`, Beam
              # binds the publish time returned by the Pub/Sub server for each message
              # to the element's timestamp parameter, accessible via `DoFn.TimestampParam`.
              # https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.ReadFromPubSub
            | "Read from Pub/Sub" >> io.ReadFromPubSub(topic=input_topic)
            | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards)
            | 'Tuple to Bigquery Row' >> beam.Map(lambda s : parse_method(s))
            | "Write to Bigquery" >> beam.io.Write(
                                                    beam.io.WriteToBigQuery (
                                                        'stream_data',
                                                        dataset = 'dataset_vw',
                                                        project = 'poc-analytics-ai',
                                                        schema = 'batch_num:STRING,message:STRING,send_time:TIMESTAMP',
                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND

                                                    )
        )
        )
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
          "--input_topic",
          help="The Cloud Pub/Sub topic to read from."
          '"projects//topics/".',
      )
    parser.add_argument(
          "--window_size",
          type=float,
          default=1.0,
          help="Output file's window size in minutes.",
      )
    parser.add_argument(
          "--output_path",
          help="Path of the output Bigquery ",
      )
    parser.add_argument(
          "--num_shards",
          type=int,
          default=5,
          help="Number of shards to use when writing windowed elements to GCS.",
      )
    known_args, pipeline_args = parser.parse_known_args()
    run(
          known_args.input_topic,
          known_args.output_path,
          known_args.window_size,
          known_args.num_shards,
          pipeline_args,
      )