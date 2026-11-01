import json
import time
import random
import os
import uuid
import urllib.request
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = 'weather-data'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

mapping_code = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Light freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    66: "Light freezing rain",
    67: "Heavy freezing rain",
    71: "Slight snow fall",
    73: "Moderate snow fall",
    75: "Heavy snow fall",
    77: "Snow grains",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail"
};

def fetch_real_weather():

    print("ambil data API dari meteo")
    url = 'https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m,weather_code,relative_humidity_2m&current=temperature_2m,relative_humidity_2m,weather_code&timezone=Asia%2FSingapore'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})

    try:
        response = urllib.request.urlopen(req)
        data = json.loads(response.read())

        current = data.get("current", {})
        return {
            "temperature" : current.get("temperature_2m"),
            "humidity" : current.get("relative_humidity_2m"),
            "weather_code" : current.get("weather_code")
        }
    except Exception as e:
        print("Data gagal diambil dari api {e}")
        return [{"temperature": 25.0, "humidity": 80.0, "weather_code": 3}]

def generated_whater_stream():

    while True:
        raw_weather = fetch_real_weather()

        #ambil 5% untul uji DLQ
        is_dirty = random.random() < 0.05

        if is_dirty:
            temp = -999.0
            humidity = -1.0
            weather_code = -1
        else:
            temp = raw_weather["temperature"]
            humidity = raw_weather["humidity"]
            weather_code = raw_weather["weather_code"]

        description = mapping_code.get(weather_code, "Unknown / Invalid")

        payload = {
            "city" : "Singapore",
            "temperature" : temp,
            "humidity" : humidity,
            "weather_code" : weather_code,
            "description" : description,
            "timestamp" : datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
        }

        try:
            producer.send(TOPIC_NAME,payload)
            print(f"Mengiri data {payload}")
        except Exception as e:
            print(f"Gagal mengirim data {e}")

        time.sleep(1)

if __name__ == "__main__":
    generated_whater_stream()


    