from kafka import KafkaProducer
import requests


kafka_data_producers = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8') )

while True:
    response_data = requests.get("http://api.openweathermap.org/data/2.5/weather?q=Lagos&appid=d3fa1aab746821d5aa1a625031b2e504")
    data = {'Lagos' : response_data.text}
    kafka_data_producers.send('weather', value=data)
    print(data)
    print()