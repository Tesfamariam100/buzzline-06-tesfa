from kafka import KafkaProducer, KafkaConsumer
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "DESKTOP-LCL0QGS:9092")  # Use hostname or set env var
KAFKA_TOPIC = "electricity_topic"  # Hardcoded topic name

def test_kafka_connection():
    try:
        # Producer Test
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

        test_message = {
            "timestamp": time.time(),
            "power_usage_kwh": 1.5,
            "rate_per_kwh": 0.12,
            "total_cost": 0.18
        }

        producer.send(KAFKA_TOPIC, value=test_message)
        producer.flush()
        print("✅ Producer successfully sent message.")
        producer.close() #close the producer

        # Consumer Test
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000 # 5 second timeout
        )

        message = next(consumer, None) #get one message, or None if timeout
        consumer.close() #close consumer.

        if message and message.value:
            print("✅ Consumer successfully received message:")
            print(json.dumps(message.value, indent=2))
            return True
        else:
            print("❌ No message received by consumer within timeout.")
            return False

    except Exception as e:
        print(f"❌ Kafka test failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing Kafka connection...")
    if test_kafka_connection():
        print("✅ Kafka connection test successful!")
    else:
        print("❌ Kafka connection test failed.")