import urllib.parse
import requests
from params import API_KEY_WEATHERBIT,BASE_URI_WEATHERBIT


#Get air quality data from API
def get_AQ_data(lat='35.5',lon='78.0',start_date='2022-03-24',end_date='2022-03-26'):
    url = urllib.parse.urljoin(BASE_URI_WEATHERBIT, f"/history/airquality/")
    params = {'lat':lat,'lon':lon,'start_date':start_date,'end_date':end_date,'tz':'local','key':API_KEY_WEATHERBIT}
    results = requests.get(url, params=params).json()['data']
    return results