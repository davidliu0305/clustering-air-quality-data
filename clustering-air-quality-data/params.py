# project id
PROJECT_ID='clustering-air-quality-data'

# bucket name
BUCKET_NAME='bucket-of-david'


TABLE_SCHEMA = 'timestamp:TIMESTAMP,pm25:FLOAT,pm10:FLOAT,so2:FLOAT,no2:FLOAT,\
    co:FLOAT,o3:FLOAT,temp:FLOAT,pres:FLOAT,dewp:FLOAT,\
    rain:FLOAT,wd:STRING,wpsm:FLOAT,station:STRING'
    
TABLE_SCHEMA_API='date:DATE,aqi:FLOAT,pm10:FLOAT,pm25:FLOAT,o3:FLOAT,so2:FLOAT,no2:FLOAT,co:FLOAT'    
    
API_KEY_WEATHERBIT='455232252a3341ebad198adcfa3ae1c3'

BASE_URI_WEATHERBIT = "http://api.weatherbit.io"   