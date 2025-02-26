# buzzline-06-tesfa

## Real-Time Power Usage Monitor
Home Electricity Cost Calculator: A real-time system that monitors power usage, calculates costs, and visualizes electricity consumption using Kafka. ⚡📊

### Overview
This project implements a real-time power usage monitoring system that:

* Simulates power meter readings.
* Streams data using Apache Kafka.
* Consumes data and calculates electricity costs.
* Supports real-time data transmission.

### Project Structure
```
streaming-power-monitor/
├── producer/         # Data generation and Kafka producer
├── consumer/           # Data processing and visualization
├── utils/             # Shared utilities
├── data/              # Data files and storage
├── requirements.txt   # Python dependencies
├── .env              # Configuration settings
└── README.md         # Project documentation
├── electricity_data.db  # store your data
├── test_kafka.py      # to test kafka connectivity 
└── .env.example       # sample for email alert config
```

### Setup Instructions

1. Clone the repository:

```python
git clone https://github.com/Tesfamariam100/buzzline-06-tesfa.git
cd buzzline-06-tesfa
```

2. Create and activate a virtual environment:

```
python -m venv .venv
.venv\Scripts\activate  # On Windows
```

3. Install dependencies:
```
pip install -r requirements.txt
```

4. Configure environment variables:

Create a .env file in the project root (if not already present).

Define Kafka settings and other configurations as needed.

### Usage
1. Start Kafka:

Ensure that Zookeeper and Kafka are running before launching the producer and consumer.

2. Create the Kafka topic:

```
in\windows\kafka-topics.bat --create --topic electricity_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

3. Run the Producer:
```
python producer/producer_tesfa.py
```

4. Run the Consumer:
```
python consumer/consumer_tesfa.py
```
### Real-time Visualization

Use Matplotlib to create a dynamic chart, visualizing real-time power consumption from the `electricity_topic`.
The consumer script updates the plot with incoming data, providing immediate visual feedback.

```python
# Example snippet from consumer.py
ax.plot(x_data, y_data, marker='o', linestyle='-')
```
### Historical Data
treamed data is stored in a database, enabling historical trend analysis, anomaly detection, and reporting.
The consumer persists each data point.
```
# Example of database interaction (conceptual)
db.insert(timestamp, power_usage)
```
### Features
* ⚡ Real-time power usage monitoring
* 💰 Cost calculation based on live data
* 🔄 Dynamic streaming with Kafka
* Scalable consumer-producer architecture
* 🚨 Add real-time alerts for abnormal power consumption.

