from confluent_kafka import Producer
from dotenv import load_dotenv
import os
import requests
import logging
import json
import time

load_dotenv()
logging.basicConfig(format='%(asctime)s: %(levelname)s - %(message)s', level=logging.INFO)

def fetch_aviation_api_data():
    url = "https://opensky-network.org/api/states/all"
    USERNAME=os.getenv("USERNAME")
    PASSWORD=os.getenv("PASSWORD")
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"API Fetch Error: {e}")
        return None

def delivery_report(err, msg):
    # returning an error in callback in case of errors
    if err:
        logging.error(f"Message delivery failed: {err}")

def produce_aviation_data():
    try:
        producer_config = {
            "bootstrap.servers": "localhost:9092",
        }
        logging.info("Fetching aviation data")
        with Producer(producer_config) as producer:
            while True:
                data = fetch_aviation_api_data()

                if data and data['states']:
                    for state in data['states']:
                        flight_event = {
                            "icao24": state[0],
                            "callsign": state[1].strip() if state[1] else None,
                            "origin_country": state[2],
                            "longitude": state[5],
                            "latitude": state[6],
                            "altitude": state[7],
                            "on_ground": state[8],
                            "velocity": state[9],
                            "timestamp": data['time']
                        }

                        # Producing aviation to aviation_raw topic for each flight
                        value = json.dumps(flight_event).encode('utf-8')
                        producer.produce(topic='aviation_raw', value=value, callback=delivery_report)

                    logging.info(f"Produced {len(data['states'])} flight events. Sleeping...")
                    time.sleep(20)
                else:
                    logging.warning("No data received or rate limit hit.")

    except KeyboardInterrupt:
        logging.error("Producer is stopped by user")

if __name__ == "__main__":
    produce_aviation_data()