import json
from kafka import KafkaConsumer
from utils.logger import setup_logger
import os
from dotenv import load_dotenv
import sys
import sqlite3

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
DATABASE_PATH = os.getenv("DATABASE_PATH", "electricity_data.db")
ABNORMAL_THRESHOLD = float(os.getenv("ABNORMAL_THRESHOLD", 5.0))  # Threshold in kWh

logger = setup_logger(__name__)

# Initialize SQLite database
def init_db():
    conn = sqlite3.connect(DATABASE_PATH)
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

init_db()

def calculate_cost(power_usage_kwh, rate=0.15):
    return power_usage_kwh * rate


# Connect to the database
conn = sqlite3.connect('data/electricity_data.db') # the line you asked about goes here.
cursor = conn.cursor()
def process_message(message):
    try:
        data = json.loads(message.value.decode("utf-8"))
        logger.info(f"Received message: {data}")

        timestamp = data.get("timestamp")
        power_usage_kwh = data.get("power_usage_kwh")

        if timestamp is None or power_usage_kwh is None:
            logger.error(f"Missing timestamp or power_usage_kwh in message: {data}")
            return

        cost = calculate_cost(power_usage_kwh)

        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO electricity_usage (timestamp, power_usage_kwh, cost) VALUES (?, ?, ?)
        ''', (timestamp, power_usage_kwh, cost))
        conn.commit()
        conn.close()

        logger.info(f"Processed and stored data: timestamp={timestamp}, power_usage_kwh={power_usage_kwh}, cost={cost}")

        # Check for abnormal consumption
        if power_usage_kwh > ABNORMAL_THRESHOLD:
            logger.warning(f"ALERT: Abnormal power consumption detected! power_usage_kwh={power_usage_kwh}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON: {message.value}")
    except Exception as e:
        logger.exception(f"Error processing message: {e}")

def main():
    try:
        consumer = KafkaConsumer(
            "electricity_topic",
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000
        )
        for message in consumer:
            process_message(message)

    except Exception as e:
        logger.exception("Consumer error:")
    finally:
        if consumer:
            consumer.close()
            logger.info("Consumer closed.")

if __name__ == "__main__":
    main()