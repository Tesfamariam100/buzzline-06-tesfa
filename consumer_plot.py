import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "electricity_topic")

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest"
)

# Matplotlib setup
fig, ax = plt.subplots()
x_data, y_data = [], []

def update_graph(i):
    try:
        message = next(consumer)  # Get the next Kafka message
        data = message.value
        print(f"Received Data: {data}")

        # Extract data
        timestamp = data.get("timestamp")
        power_usage = data.get("power_usage")

        if timestamp and power_usage is not None:
            x_data.append(timestamp)
            y_data.append(power_usage)

            # Keep only the last 20 readings
            if len(x_data) > 20:
                x_data.pop(0)
                y_data.pop(0)

            ax.clear()
            ax.plot(x_data, y_data, marker='o', linestyle='-')
            ax.set_title("Real-time Electricity Consumption")
            ax.set_xlabel("Time")
            ax.set_ylabel("Power Usage (kW)")
            plt.xticks(rotation=45)
    except StopIteration:
        pass

# Start real-time visualization
ani = animation.FuncAnimation(fig, update_graph, interval=1000)
plt.show()
