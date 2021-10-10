from kafka import KafkaConsumer
import json
import requests
from datetime import datetime
import time
from json import dumps
import pprint
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter

register_adapter(dict, json)


connection = config.database_credentials

cursor = connection.cursor()

weather_kafka_consumer = KafkaConsumer('weather', bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest', value_deserializer=lambda m: json.loads(m.decode('utf-8')))


for i in weather_kafka_consumer:
    data = i.value
    output = json.loads(data['Lagos'])  
    
    clean_output = [{
    'lon': float(output['coord']['lon']),
    'lat': float(output['coord']['lat']),
    'weather_id': str(output['weather'][0]['id']),
    'weather_main': str(output['weather'][0]['main']),
    'weather_icon': str(output['weather'][0]['icon']),
    'weather_description': str(output['weather'][0]['description']),
    'weather_base': str(output['base']),
    'temp': float(output['main']['temp']),
    'feels_like': float(output['main']['feels_like']),
    'temp_min': float(output['main']['temp_min']),
    'temp_max': float(output['main']['temp_max']),
    'pressure': int(output['main']['pressure']),
    'humidity': int(output['main']['humidity']),
    'sea_level': int(output['main']['sea_level']),
    'grnd_level': int(output['main']['grnd_level']),
    'visibility': int(output['visibility']),
    'wind_speed': float(output['wind']['speed']),
    'wind_deg': int(output['wind']['deg']),
    'wind_gust': float(output['wind']['gust']),
    'cloud_all': int(output['clouds']['all']),
    'dt': int(output['dt']),
    'sys_tire': int(output['sys']['type']),
    'sys_id': int(output['sys']['id']),
    'sys_country': str(output['sys']['country']),
    'sys_sunrise': int(output['sys']['sunrise']),
    'sys_sunset': int(output['sys']['sunset']),
    'timezone': int(output['timezone']),
    'id': int(output['id']),
    'name': str(output['name']),
    'cod': int(output['cod'])
    }]

  
    columns = clean_output [0].keys()
    query = "INSERT INTO weather_data ({}) VALUES %s".format(','.join(columns))

    # convert projects values to sequence of seqeences
    values = [[value for value in item.values()] for item in clean_output]
    execute_values(cursor, query, values)
    connection.commit()

    ## TODO 2 
    #SEND to Power realtime

