import json
from kafka import KafkaConsumer
from utils.logger import setup_logger

logger = setup_logger(__name__)

def process_message(message):
    try:
        data = json.loads(message.value.decode("utf-8"))
        logger.info(f"Received message: {data}")
        # Process the data (e.g., calculations, visualization)
        # ... your logic here ...
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON: {message.value}")
    except Exception as e:
        logger.exception(f"Error processing message: {e}") # Log the full traceback

def main():
    try:
        consumer = KafkaConsumer(
            "electricity_topic",  # Your topic name
            bootstrap_servers=["localhost:9092"], # Your Kafka broker address
            auto_offset_reset="earliest",  # Start from the beginning of the topic
            consumer_timeout_ms=1000  # Timeout if no message is received
        )
        for message in consumer:
            process_message(message)

    except Exception as e:
        logger.exception("Consumer error:") # Log the full traceback
    finally:
        if consumer:
            consumer.close()
            logger.info("Consumer closed.")

if __name__ == "__main__":
    main()