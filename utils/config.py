from dotenv import load_dotenv
import os

# Load variables from .env file
load_dotenv()

# Access the variables
kafka_broker = os.getenv("KAFKA_BROKER")
email_sender = os.getenv("EMAIL_SENDER")

print(f"Kafka Broker: {kafka_broker}")
print(f"Email Sender: {email_sender}")
