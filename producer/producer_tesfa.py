import json
import time
from kafka import KafkaProducer
from utils.logger import setup_logger
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

logger = setup_logger(__name__)

def generate_reading():
    power_usage = 10 + (time.time() % 50)
    rate_per_kwh = 0.15
    total_cost = power_usage * rate_per_kwh
    return {
        "timestamp": time.time(),
        "power_usage_kwh": power_usage,
        "rate_per_kwh": rate_per_kwh,
        "total_cost": total_cost,
    }

def send_to_kafka(data, producer):
    try:
        producer.send(KAFKA_TOPIC, value=data)
        logger.info(f"Sent message: {data}")
    except Exception as e:
        logger.error(f"Error sending message: {e}")

def main():
    try:
        producer = KafkaProducer(bootstrap_servers=[os.getenv("KAFKA_BROKER", "DESKTOP-LCL0QGS:9092")], value_serializer=lambda x: x)
        while True:
            reading = generate_reading()
            message = json.dumps(reading).encode('utf-8')
            send_to_kafka(message, producer)
            time.sleep(2)
    except Exception as e:
        logger.exception("Producer error:")
    finally:
        if producer:
            producer.close()
            logger.info("Producer closed.")

if __name__ == "__main__":
    main()