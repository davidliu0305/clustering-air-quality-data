# from os.path import isfile
# from os.path import dirname

# version_file = '{}/version.txt'.format(dirname(__file__))

# if isfile(version_file):
#     with open(version_file) as version_file:
#         __version__ = version_file.read().strip()

from pipeline import get_pipeline
import apache_beam as beam
from params import BUCKET_NAME

def split_csv_line(element):
    return element.split(',')


if __name__ == "__main__":
    p = get_pipeline()
    process = (
           p 
           | 'Read data' >> beam.io.ReadFromText(f'gs://{BUCKET_NAME}/*.csv',skip_header_lines=1)
           | 'Split csv lines' >> beam.Map(split_csv_line)
        #    | 'Write data' >> beam.io.WriteToText("../raw_data/test.csv")
          )
    result = p.run()
    result.wait_until_finish()
