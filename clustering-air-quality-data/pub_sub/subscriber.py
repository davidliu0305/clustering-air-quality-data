import apache_beam as beam
from pipeline import get_pipeline



def __convert_to_tuple(element):
    return (element['timestamp_local'].split('T')[0],{
        "aqi": element['aqi'],
        "pm10": element['pm10'],
        "pm25": element['pm25'],
        "o3": element['o3'],
        "so2": element['so2'],
        "no2": element['no2'],
        "co": element['co'],
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
    p = get_pipeline()