import json
import time
from kafka import KafkaProducer, KafkaConsumer

def test_kafka_connection():
    KAFKA_BROKER = '172.21.198.224:9092'
    TOPIC_NAME = 'electricity_topic'

    producer = None  # Initialize to None for finally block
    consumer = None  # Initialize to None for finally block

    try:
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

        producer.send(TOPIC_NAME, value=test_message)
        producer.flush()
        print("✅ Producer successfully connected and sent message")

        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',  # Now 'earliest'
            group_id='my_test_group',       # Add a group ID
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        print("Waiting for messages... (10 second timeout)")

        messages = consumer.poll(timeout_ms=10000)
        if messages:
            for _, msgs in messages.items():
                for msg in msgs:
                    if msg and msg.value: # Check if message and value are not None
                        print("✅ Consumer successfully connected and received message:")
                        print(f"Received: {json.dumps(msg.value, indent=2)}")
                        return True
                    else:
                        print("Received empty message")
            return False # No non-empty message received
        else:
            print("❌ No message received in consumer test")
            return False

    except (ConnectionError, TimeoutError) as e:
        print(f"❌ Kafka connection error: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Kafka test failed: {str(e)}")
        return False
    finally:
        if producer:
            producer.close()
        if consumer:
            consumer.close()

if __name__ == "__main__":
    print("Testing Kafka connection to WSL...")
    test_kafka_connection()