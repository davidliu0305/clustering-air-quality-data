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
    
    ts = datetime(int(fields[1]),int(fields[2]),int(fields[3]),int(fields[4]),0,0,0).strftime('%Y-%m-%d %H:%M:%S.%f')
    
    json_str = { 
                 "timestamp": ts,
                 "pm25": float(fields[5]) if str(fields[5]).strip('-').isnumeric() else None,
                 "pm10": float(fields[6]) if str(fields[6]).strip('-').isnumeric() else None,
                 "so2": float(fields[7]) if str(fields[7]).strip('-').isnumeric() else None,
                 "no2": float(fields[8]) if str(fields[8]).strip('-').isnumeric() else None,
                 "co": float(fields[9]) if str(fields[9]).strip('-').isnumeric() else None,
                 "o3": float(fields[10]) if str(fields[10]).strip('-').isnumeric() else None,
                 "temp": float(fields[11]) if str(fields[11]).strip('-').isnumeric() else None,
                 "pres": float(fields[12]) if str(fields[12]).strip('-').isnumeric() else None,
                 "dewp": float(fields[13]) if str(fields[13]).strip('-').isnumeric() else None,
                 "rain": float(fields[14]) if str(fields[14]).strip('-').isnumeric() else None,
                 "wd":str(fields[15]) if str(fields[15]) != 'NA' else None,
                 "wpsm":float(fields[16]) if str(fields[16]).strip('-').isnumeric() else None,
                 "station": str(fields[17]) if str(fields[17]) != 'NA' else None
                 }

    return json_str

def __run():
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


if __name__ == "__main__":
    __run()
