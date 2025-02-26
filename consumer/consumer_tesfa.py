import json
from kafka import KafkaConsumer
from utils.logger import setup_logger
import os
from dotenv import load_dotenv
import sys
import sqlite3  # Import SQLite for data storage

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))

load_dotenv()  # Load environment variables from .env file

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
DATABASE_PATH = os.getenv("DATABASE_PATH", "electricity_data.db") # Default database path

logger = setup_logger(__name__)

# Initialize SQLite database
def init_db():
    conn = sqlite3.connect('data/electricity_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS electricity_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            power_usage_kwh REAL,
            cost REAL
        )
    ''')
    conn.commit()
    conn.close()

init_db()  # Initialize the database when the consumer starts

def calculate_cost(power_usage_kwh, rate=0.15):  # Example rate
    return power_usage_kwh * rate

def process_message(message):
    try:
        data = json.loads(message.value.decode("utf-8"))
        logger.info(f"Received message: {data}")

        # Extract data
        timestamp = data.get("timestamp")
        power_usage_kwh = data.get("power_usage_kwh")

        if timestamp is None or power_usage_kwh is None:
            logger.error(f"Missing timestamp or power_usage_kwh in message: {data}")
            return # exit the function if data is missing.

        # Calculate cost
        cost = calculate_cost(power_usage_kwh)

        # Store data in SQLite
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO electricity_usage (timestamp, power_usage_kwh, cost) VALUES (?, ?, ?)
        ''', (timestamp, power_usage_kwh, cost))
        conn.commit()
        conn.close()

        logger.info(f"Processed and stored data: timestamp={timestamp}, power_usage_kwh={power_usage_kwh}, cost={cost}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON: {message.value}")
    except Exception as e:
        logger.exception(f"Error processing message: {e}")  # Log the full traceback

def main():
    try:
        consumer = KafkaConsumer(
            "electricity_topic",  # Your topic name
            bootstrap_servers=["localhost:9092"],  # Your Kafka broker address
            auto_offset_reset="earliest",  # Start from the beginning of the topic
            consumer_timeout_ms=1000  # Timeout if no message is received
        )
        for message in consumer:
            process_message(message)

    except Exception as e:
        logger.exception("Consumer error:")  # Log the full traceback
    finally:
        if consumer:
            consumer.close()
            logger.info("Consumer closed.")

if __name__ == "__main__":
    main()