import json
import time
from kafka import KafkaProducer
from utils.logger import setup_logger  # Assuming you have a logger setup

import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

logger = setup_logger(__name__)

def generate_reading():
    power_usage = 10 + (time.time() % 50)  # Example: varies between 10 and 60
    rate_per_kwh = 0.15  # Example rate
    total_cost = power_usage * rate_per_kwh
    return {  # Return the dictionary directly
        "timestamp": time.time(),
        "power_usage_kwh": power_usage,
        "rate_per_kwh": rate_per_kwh,
        "total_cost": total_cost,
    }

def send_to_kafka(data, producer):
    try:
        message = json.dumps(data).encode("utf-8")
        producer.send("electricity_topic", message)  # Use your topic name
        logger.info(f"Sent message: {data}")
    except Exception as e:
        logger.error(f"Error sending message: {e}")

def main():
    try:
        producer = KafkaProducer(bootstrap_servers=["localhost:9092"]) # Your Kafka broker address
        while True:
            reading = generate_reading()
            send_to_kafka(reading, producer)
            time.sleep(2)  # Send a reading every 2 seconds
    except Exception as e:
        logger.exception("Producer error:") # Log the full traceback
    finally:
        if producer:
            producer.close()
            logger.info("Producer closed.")

if __name__ == "__main__":
    main()