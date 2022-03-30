import apache_beam as beam

data = [
{
"aqi": 37,
"pm10": 2.07,
"pm25": 1.55,
"o3": 79,
"timestamp_local": "2022-03-27T00:00:00",
"so2": 1.67,
"no2": 2.79,
"timestamp_utc": "2022-03-27T04:00:00",
"datetime": "2022-03-27:04",
"co": 255.15,
"ts": 1648353600
},
{
"aqi": 37,
"pm10": 1.99,
"pm25": 1.5,
"o3": 79,
"timestamp_local": "2022-03-26T23:00:00",
"so2": 1.6,
"no2": 2.93,
"timestamp_utc": "2022-03-27T03:00:00",
"datetime": "2022-03-27:03",
"co": 255.03,
"ts": 1648350000
}]

#convert to tuple for a day
def convert_to_tuple(element):
    return (element['timestamp_local'].split('T')[0],{
        "aqi": element['aqi']/len(data),
        "pm10": element['pm10']/len(data),
        "pm25": element['pm25']/len(data),
        "o3": element['o3']/len(data),
        "so2": element['so2']/len(data),
        "no2": element['no2']/len(data),
        "co": element['co']/len(data)
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
    with beam.Pipeline() as pipeline:
        total = (
        pipeline
        | 'input data' >> beam.Create(data)
        | 'convert to tuple' >> beam.Map(convert_to_tuple)
        | 'final results' >> beam.CombinePerKey(MergeDictCombineFn())
        | beam.Map(print))
          

