from params import BUCKET_NAME, PROJECT_ID
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import SetupOptions
import argparse

class MyOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    parser = argparse.ArgumentParser()
    parser.add_argument('--runner', default='dataflow')
    parser.add_argument('--project', default=PROJECT_ID)
    parser.add_argument('--temp_location', default=f"gs://{BUCKET_NAME}/temp")
    parser.add_argument('--staging_location', default=f"gs://{BUCKET_NAME}/staging")
    parser.add_argument('--region', default='us-central1')

def get_pipeline():
    pipeline_options = PipelineOptions().view_as(MyOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = Pipeline(options=pipeline_options)
    return p


    
    
    


      
      
   