# Real-Time CDR Fraud Detection System

A production-grade real-time fraud detection system for telecom Call Detail Records (CDR) using Apache Kafka, Python, PostgreSQL, and Redis.

## Project Overview

This system processes telecom CDR events in real-time, detecting fraudulent patterns and generating alerts within seconds. Built as a proof-of-concept to demonstrate stream processing capabilities for enterprise telecom fraud prevention.

## Features

- **Real-time Processing**: Processes 10+ events/second with sub-100ms latency
- **Fraud Detection**: 
  - High revenue calls (premium rate fraud)
  - Abnormal call duration (> 30 minutes)
  - Excessive data usage (> 50MB sessions)
- **Multi-CDR Support**: Voice calls, SMS, and data sessions
- **Complete Monitoring**: Kafka UI, PostgreSQL analytics, Redis metrics
- **Scalable Architecture**: Kafka partitioning for parallel processing

## Architecture
```
CDR Generator (Python)
    ↓
Kafka Topic: cdr-events (3 partitions)
    ↓
Consumer (Python) - Real-time fraud detection
    ↓
    ├→ PostgreSQL (Analytics storage)
    ├→ Redis (Real-time metrics)
    └→ Kafka Topic: cdr-alerts (Fraud alerts)
```

## Tech Stack

- **Apache Kafka 7.5.0** - Event streaming platform
- **Python 3.8+** - Application logic
- **PostgreSQL 15** - Analytics database
- **Redis 7** - Real-time metrics cache
- **Docker Compose** - Infrastructure orchestration

## Prerequisites

- Docker Desktop 20.10+
- Docker Compose 1.29+
- Python 3.8+
- Git

## Installation

### 1. Clone Repository
```bash
git clone https://github.com/ezechimere/cdr-fraud-detection.git
cd cdr-fraud-detection
```

### 2. Start Infrastructure
```bash
cd docker
docker-compose up -d
```

Wait 60 seconds for all services to start.

### 3. Create Kafka Topics
```bash
docker exec -it kafka bash
cd /bin

kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic cdr-events \
  --partitions 3 \
  --replication-factor 1

kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic cdr-alerts \
  --partitions 1 \
  --replication-factor 1

exit
```

### 4. Setup Database
```bash
docker cp config/init_db.sql postgres:/tmp/
docker exec -it postgres psql -U kafka_user -d cdr_analytics -f /tmp/init_db.sql
```

### 5. Install Python Dependencies
```bash
python -m venv venv
source venv/Scripts/activate  # Windows Git Bash
# or: .\venv\Scripts\Activate.ps1  # PowerShell
# or: source venv/bin/activate  # Linux/Mac

pip install -r requirements.txt
```

## Usage

### Running the System

**Terminal 1 - Start Consumer:**
```bash
python src/cdr_consumer.py
```

**Terminal 2 - Generate CDR Events:**
```bash
python src/cdr_generator.py
```

### Monitoring

- **Kafka UI**: http://localhost:8090
- **PostgreSQL**: `docker exec -it postgres psql -U kafka_user -d cdr_analytics`
- **Query Database**: `python src/query_db.py`

## Sample Output
```
CDR Consumer started. Processing events...

[OK] Processed 50 messages
[METRICS] Flushed: {'total_voice_calls': 30, 'total_revenue': 456.50}

[ALERT] high_revenue_call - 234080XXXXXXX
[ALERT] long_duration_call - 234081XXXXXXX

[OK] Processed 100 messages
```

## Database Schema

- **voice_calls** - Voice call records
- **sms_records** - SMS records
- **data_sessions** - Data usage records
- **cdr_alerts** - Fraud alerts
- **realtime_metrics** - Dashboard metrics
- **cdr_raw** - Raw event archive

## Performance Metrics

- **Throughput**: 10 events/second (scalable to thousands)
- **Latency**: < 100ms end-to-end
- **Fraud Detection**: < 1 second from event to alert
- **Reliability**: Exactly-once processing semantics

## Business Value

This system demonstrates capabilities for:

- **Fraud Prevention**: Real-time detection saves N1.7B/year for large telcos
- **Cost Reduction**: In-house solution vs N500M/year vendor costs
- **Scalability**: Handles millions of CDRs daily
- **Extensibility**: Easy to add new fraud detection rules

## Configuration

Edit `src/database.py` for database connection:
```python
self.conn_params = {
    'host': '127.0.0.1',
    'port': 5432,
    'database': 'cdr_analytics',
    'user': 'kafka_user',
    'password': 'kafka_pass'
}
```

Edit `src/cdr_generator.py` for event generation rate:
```python
generator.generate_stream(events_per_second=10, duration_seconds=60)
```

## Project Structure
```
cdr-fraud-detection/
├── src/
│   ├── cdr_generator.py       # CDR event generator
│   ├── cdr_consumer.py         # Real-time fraud detection consumer
│   ├── simple_producer.py      # Basic producer example
│   ├── simple_consumer.py      # Basic consumer example
│   ├── database.py             # PostgreSQL integration
│   └── query_db.py             # Database query utilities
├── config/
│   └── init_db.sql             # Database schema
├── docker/
│   └── docker-compose.yml      # Infrastructure setup
├── requirements.txt            # Python dependencies
├── .gitignore
└── README.md
```

## Roadmap

- Machine learning-based fraud prediction
- Behavioral analysis (deviation from user patterns)
- Premium rate destination blacklist
- Real-time Grafana dashboards
- Email/SMS alerting for critical fraud
- Multi-telco deployment capability

## Contributing

This is a portfolio/demonstration project. For production use, additional features needed:
- Authentication & authorization
- Encrypted connections
- High availability setup
- Monitoring & alerting
- Backup & disaster recovery

## License

MIT License - See LICENSE file for details

## Author

**Conrad Mba** - Software Engineer | Data Engineering & Real-Time Systems | Kafka Specialist

- LinkedIn: [https://www.linkedin.com/in/conrad-mba/]- Email: [mbaconrad@gmail.com]

## Acknowledgments

Built as part of intensive Kafka training program focused on real-time stream processing for telecom fraud detection.
