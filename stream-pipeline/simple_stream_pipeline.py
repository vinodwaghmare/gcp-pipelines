import argparse
import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window
from datetime import datetime as dt

def run(input_topic, output_path, pipeline_args=None):
    pipeline_options = PipelineOptions(pipeline_args, streaming=True, save_main_session=True)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p | 'Read event stream from pub sub topic' >> beam.io.ReadFromPubSub(topic=input_topic)
              | 'Converts from bytes to string' >> beam.Map(lambda x : x.decode('utf-8'))
              | 'Events data' >> 
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
        "--output_path",
        help="Path of the output GCS file including the prefix.",
    )
    known_args, pipeline_args = parser.parse_known_args()
    run(
        known_args.input_topic,
        known_args.output_path,
        pipeline_args
    )
