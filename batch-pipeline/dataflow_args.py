import argparse
import logging
import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import json


# Entry run method for triggering pipline
def run():
    #Input arguments , reading from commandline
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://test-bucket-vw',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args()

    # Function to parse and format given input to Big Query readable JSON format
    def parse_method(string_input):

        values = re.split(",",re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        row = dict(
            zip(('IATA_CODE', 'AIRPORT', 'CITY','STATE','COUNTRY','LATITUDE','LONGITUDE'),
                values))
        return row


    
    # Main Pipeline 
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        input_data = p | 'read' >> ReadFromText(known_args.input,skip_header_lines=1)
        string = input_data| 'To String' >> beam.Map(lambda s: str(s))
        parsed = string | 'Parse lines' >> beam.Map(lambda s : parse_method(s))
        # Write to Big Query Sink
        parsed | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                                                                             table='poc-analytics-ai:dataset_vw.batchdata',
                                                                             schema='IATA_CODE:STRING,AIRPORT:STRING,CITY:STRING,STATE:STRING,COUNTRY:STRING,LATITUDE:FLOAT,LONGITUDE:FLOAT',
                                                                             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                                                                        )
                                                    
# Trigger entry function here       
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
