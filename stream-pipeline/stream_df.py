from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
import google.auth
from apache_beam import window
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions(project = 'poc-analytics-ai')