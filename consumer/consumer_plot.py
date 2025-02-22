import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "DESKTOP-LCL0QGS:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "electricity_topic")

# Initialize Kafka Consumer with error handling
def deserialize_message(v):
    try:
        return json.loads(v.decode("utf-8"))
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=deserialize_message,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    consumer_timeout_ms=60000
)

# Matplotlib setup
plt.ion()
fig, ax = plt.subplots()
x_data, y_data = [], []

def update_graph(i):
    try:
        message = next(consumer)
        if message and message.value:
            data = message.value
            if isinstance(data, dict):
                print(f"Received Data: {data}")
                timestamp = data.get("timestamp")
                power_usage = data.get("power_usage_kwh") #Changed to match producer.

                if isinstance(timestamp, (int, float)) and isinstance(power_usage, (int, float)):
                    x_data.append(timestamp)
                    y_data.append(power_usage)

                    if len(x_data) > 20:
                        x_data.pop(0)
                        y_data.pop(0)

                    ax.clear()
                    ax.plot(x_data, y_data, marker='o', linestyle='-')
                    ax.set_title("Real-time Electricity Consumption")
                    ax.set_xlabel("Time")
                    ax.set_ylabel("Power Usage (kW)")
                    plt.xticks(rotation=45)

                    if len(y_data) > 0:
                        ax.set_ylim(min(y_data) - 0.5, max(y_data) + 0.5)

                    plt.draw()
                    plt.pause(0.1)
            else:
                print("Received non-dictionary data")
        elif message is None:
            print("Consumer timeout")
    except StopIteration:
        print("Kafka stream stopped")

global anim
anim = animation.FuncAnimation(fig, update_graph, interval=1000, cache_frame_data=False)

plt.show(block=True)