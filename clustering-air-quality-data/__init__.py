# from os.path import isfile
# from os.path import dirname

# version_file = '{}/version.txt'.format(dirname(__file__))

# if isfile(version_file):
#     with open(version_file) as version_file:
#         __version__ = version_file.read().strip()

from pipeline import get_pipeline
import apache_beam as beam
from params import BUCKET_NAME, TABLE_SCHEMA
from google.cloud import bigquery
from datetime import datetime
import re

def __remove_special_characters(row):    
    import re
    cols = row.split(',')			
    ret = ''
    for col in cols:
        clean_col = re.sub(r'[?%&]','', col)
        ret = ret + clean_col + ','			
    ret = ret[:-1]						
    return ret


def __to_json(csv_str):
    fields = csv_str.split(',')
    #timestamp
    ts = datetime(int(fields[1]),int(fields[2]),int(fields[3]),int(fields[4]),0,0,0).strftime('%Y-%m-%d %H:%M:%S.%f')
    
    #regular expression for both int and floating numbers
    regex = '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
    
    json_str = { 
                 "timestamp": ts,
                 "pm25": float(fields[5]) if re.search(regex, fields[5]) else None,
                 "pm10": float(fields[6]) if re.search(regex, fields[6]) else None,
                 "so2": float(fields[7]) if re.search(regex, fields[7]) else None,
                 "no2": float(fields[8]) if re.search(regex, fields[8]) else None,
                 "co": float(fields[9]) if re.search(regex, fields[9]) else None,
                 "o3": float(fields[10]) if re.search(regex, fields[10]) else None,
                 "temp": float(fields[11]) if re.search(regex, fields[11]) else None,
                 "pres": float(fields[12]) if re.search(regex, fields[12]) else None,
                 "dewp": float(fields[13]) if re.search(regex, fields[13]) else None,
                 "rain": float(fields[14]) if re.search(regex, fields[14]) else None,
                 "wd":str(fields[15]) if str(fields[15]) != 'NA' else None,
                 "wpsm":float(fields[16]) if re.search(regex, fields[16]) else None,
                 "station": str(fields[17]) if str(fields[17]) != 'NA' else None
                 }

    return json_str

#extract data from bucket
def __run_bucket():
    p = get_pipeline()
    client = bigquery.Client()
    dataset_id = "clustering-air-quality-data.air_quality_data"

    dataset = bigquery.Dataset(dataset_id)

    dataset.location = "US"
    dataset.description = "dataset for air quality data"
    dataset_ref = client.create_dataset(dataset_id, exists_ok=True)

    read_data = (
           p 
           | 'Read data' >> beam.io.ReadFromText(f'gs://{BUCKET_NAME}/PRSA_Data_Aotizhongxin_20130301-20170228.csv',skip_header_lines=1) 
           | 'Remove special characters' >> beam.Map(__remove_special_characters)
           | 'Split csv lines' >> beam.Map(__to_json)
        #    | 'Write data' >> beam.io.WriteToText("../raw_data/test.csv")
           | 'Write to BigQuery' >> beam.io.WriteToBigQuery(schema=TABLE_SCHEMA,
                    table='clustering-air-quality-data:air_quality_data.measurement',
                    custom_gcs_temp_location=f"gs://{BUCKET_NAME}/temp/",                                        
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
          )
    result = p.run()
    result.wait_until_finish()
    
    
#extract data from api
def __run_api():
    pass

if __name__ == "__main__":
    __run_bucket()
