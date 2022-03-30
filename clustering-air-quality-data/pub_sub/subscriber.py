import apache_beam as beam
from pipeline import get_pipeline
from google.cloud import bigquery
from params import PROJECT_ID
import json
from apache_beam.transforms.trigger import AccumulationMode, AfterCount,Repeatedly

# Replace with your input subscription id
input_subscription = f'projects/{PROJECT_ID}/subscriptions/Subscribe_AQ'

def __decode_string(element):
    return json.loads(element.decode('utf-8').replace("\'",'\"'))

def __convert_to_tuple(element):
    return (element['timestamp_local'].split('T')[0],{
        "aqi": element['aqi']/24,
        "pm10": element['pm10']/24,
        "pm25": element['pm25']/24,
        "o3": element['o3']/24,
        "so2": element['so2']/24,
        "no2": element['no2']/24,
        "co": element['co']/24,
        })

class MergeDictCombineFn(beam.CombineFn):

    def _sum_up(self, elements, accumulator=None):
        accumulator = accumulator or self.create_accumulator()
        for obj in elements:
            for k, v in obj.items():
                if k not in accumulator:
                    accumulator[k] = 0
                accumulator[k] += float(v)
        return accumulator

    def create_accumulator(self):
        return {}

    def add_input(self, accumulator, element, *args, **kwargs):
        return self._sum_up(elements=[element], accumulator=accumulator)

    def add_inputs(self, accumulator, elements, *args, **kwargs):
        return self._sum_up(elements=elements, accumulator=accumulator)

    def merge_accumulators(self, accumulators, *args, **kwargs):
        return self._sum_up(elements=accumulators)

    def extract_output(self, accumulator, *args, **kwargs):
        return accumulator
    

if __name__ == "__main__":
    p = get_pipeline(used_by_streaming=True)
    client = bigquery.Client()
    dataset_id = "clustering-air-quality-data.air_quality_data"    
    
    read_data = (
           p 
           | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
           | 'decode string' >> beam.Map(__decode_string) 
           | 'convert to tuple' >> beam.Map(__convert_to_tuple)
           | 'Window' >> beam.WindowInto(beam.window.GlobalWindows(), \
               trigger=Repeatedly(AfterCount(24)), accumulation_mode=AccumulationMode.DISCARDING)
           | 'final results' >> beam.CombinePerKey(MergeDictCombineFn())
           | 'beam results' >> beam.Map(print)
        )    
    result = p.run()
    result.wait_until_finish()