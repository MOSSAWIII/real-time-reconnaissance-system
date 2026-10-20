> **Demonstration status:** This repository uses **mock camera-frame payloads** and short scripted test runs, not a deployed surveillance service. The numerical latency values below are **illustrative sample output**, not benchmark measurements validated on current hardware or infrastructure. Docker images and services have not been revalidated as part of this README correction.

# Real-Time Reconnaissance System with Kafka

A distributed real-time system for processing surveillance camera frames using Apache Kafka for message streaming between stream normalizers and phenomenon detectors.

## 🏛️ System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Stream          │    │ Kafka Cluster   │    │ Phenomenon      │
│ Normalizer      │───▶│                 │───▶│ Detector        │
│ (Producer)      │    │ • 6 Camera      │    │ (Consumer)      │
│                 │    │   Topics        │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Components:
- **Stream Normalizer**: Produces normalized frame events for 6 cameras
- **Kafka Cluster**: Message broker with dedicated topics per camera
- **Phenomenon Detector**: Consumes and analyzes frames from all cameras

## 🎥 Camera Configuration

The system supports 6 surveillance cameras:
- `CAM-QUAI-01` → Topic: `cam-quai-01-frames`
- `CAM-QUAI-02` → Topic: `cam-quai-02-frames`
- `CAM-QUAI-03` → Topic: `cam-quai-03-frames`
- `CAM-PORT-01` → Topic: `cam-port-01-frames`
- `CAM-PORT-02` → Topic: `cam-port-02-frames`
- `CAM-ENTRY-01` → Topic: `cam-entry-01-frames`

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- At least 4GB RAM available for containers

### 1. Start the System
```bash
# Navigate to the kafka directory
cd real-time-reconnaissance-system

# Start all services
docker-compose up --build
```

### 2. Monitor the Output
The system will:
1. Start Kafka broker with health checks
2. Launch Stream Normalizer (produces 12 events: 2 per camera)
3. Launch Phenomenon Detector (consumes all events)
4. Display latency statistics and shut down automatically

### 3. Clean Up
```bash
# Stop and remove containers
docker-compose down

# Remove volumes (optional)
docker-compose down -v
```

## 📊 Event Format

### NormalizedFrameReady Event
```json
{
  "eventId": "uuid-string",
  "timestamp": "2025-01-13T14:30:15.123456Z",
  "missionId": "MISSION-SURVEILLANCE-2025",
  "assetId": "CAM-QUAI-01",
  "frame_payload": "base64-encoded-image-data",
  "send_timestamp": 1736789012.345
}
```

## 📈 Performance Monitoring

The system automatically tracks and reports:
- **Individual Message Latency**: Time from producer send to consumer receive
- **Average/Min/Max Latency**: Statistical analysis of all messages
- **End-to-End Processing Time**: Total time from first send to last receive
- **Throughput**: Messages processed per second

### Sample Output:
```
LATENCY ANALYSIS REPORT
================================================================================
Individual Message Latencies (12 messages):
------------------------------------------------------------
 1. Event 12345678... from CAM-QUAI-01:  15.23ms
 2. Event 87654321... from CAM-PORT-02:  12.45ms
...

Latency Statistics:
------------------------------
Average Latency: 18.45ms
Minimum Latency: 8.12ms
Maximum Latency: 35.67ms
Total Messages:  12

End-to-End Processing Time:
-----------------------------------
First Message Sent:     14:30:15.123
Last Message Received:  14:30:35.456
Total Processing Time:  20333.00ms (20.33s)
```

## 🏗️ Project Structure

```
kafka/
├── README.md                 # This file
├── docker-compose.yml        # Multi-service configuration
├── normalizer/              # Stream Normalizer (Producer)
│   ├── Dockerfile
│   └── producer.py
└── detector/                # Phenomenon Detector (Consumer)
    ├── Dockerfile
    └── consumer.py
```

## ⚙️ Configuration

### Environment Variables
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: `kafka:29092`)

### Kafka Configuration
- **Replication Factor**: 1 (suitable for development)
- **Partitions**: 1 per topic
- **Auto-create Topics**: Enabled
- **Retention**: Default (7 days)

## 🔧 Development

### Running Individual Components

#### Stream Normalizer Only
```bash
cd normalizer
docker build -t stream-normalizer .
docker run --network kafka_default -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 stream-normalizer
```

#### Phenomenon Detector Only
```bash
cd detector
docker build -t phenomenon-detector .
docker run --network kafka_default -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 phenomenon-detector
```

### Local Development
```bash
# Install dependencies
pip install kafka-python

# Set environment variable
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Run components
python normalizer/producer.py
python detector/consumer.py
```

## 🐛 Troubleshooting

### Common Issues

#### 1. Kafka Connection Failed
```
[PRODUCER] Failed to connect to Kafka: NoBrokersAvailable
```
**Solution**: Wait for Kafka to fully start (health check passes)

#### 2. Topics Not Created
```
[PRODUCER] Topics may already exist: TopicAlreadyExistsError
```
**Solution**: This is normal - topics are created on first run

#### 3. Consumer Timeout
```
[CONSUMER] Timeout: No messages received after 30 cycles
```
**Solution**: Ensure producer has sent messages and topics exist

#### 4. Container Memory Issues
**Solution**: Increase Docker memory allocation to 4GB+

### Health Checks

#### Verify Kafka is Running
```bash
docker exec -it kafka_kafka_1 kafka-topics.sh --bootstrap-server localhost:9092 --list
```

#### Check Topic Messages
```bash
docker exec -it kafka_kafka_1 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cam-quai-01-frames --from-beginning
```

## 📋 System Requirements

- **Docker**: Version 20.0+
- **Docker Compose**: Version 2.0+
- **Memory**: 4GB RAM minimum
- **Storage**: 2GB free space
- **Network**: Internet access for image downloads

## 🎯 Use Cases

This system is designed for:
- **Real-time surveillance monitoring**
- **Distributed camera management**
- **Event-driven architecture testing**
- **Kafka streaming demonstrations**
- **Latency performance analysis**

## 📝 Notes

- The system sends 2 events per camera (12 total) then automatically shuts down
- Frame payloads are mock base64-encoded images
- Latency measurements include network and processing overhead
- All components use graceful shutdown with proper resource cleanup

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with `docker-compose up --build`
5. Submit a pull request

## 📄 License

This project is for educational and demonstration purposes.
