from kafka import KafkaProducer
import json
from datetime import datetime
import time

# Initialize producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

# Sample CDR data
cdr_records = [
    {
        'msisdn': '2348012345678',
        'call_type': 'voice',
        'duration': 120,
        'destination': '2348087654321',
        'cell_id': 'LAG_VI_001',
        'timestamp': datetime.now().isoformat()
    },
    {
        'msisdn': '2348087654321',
        'call_type': 'sms',
        'destination': '2348012345678',
        'cell_id': 'LAG_VI_002',
        'timestamp': datetime.now().isoformat()
    },
    {
        'msisdn': '2348012345678',
        'call_type': 'data',
        'bytes': 10485760,  # 10MB
        'cell_id': 'LAG_VI_001',
        'timestamp': datetime.now().isoformat()
    }
]

# Produce messages
print("Sending CDR events to Kafka...\n")
for record in cdr_records:
    future = producer.send(
        topic='cdr-events',
        key=record['msisdn'],  # Partition by MSISDN
        value=record
    )
    
    # Wait for confirmation
    result = future.get(timeout=10)
    print(f"[OK] Sent: {record['call_type']}")
    print(f"     MSISDN: {record['msisdn']}")
    print(f"     Partition: {result.partition}")
    print(f"     Offset: {result.offset}")
    print(f"     Timestamp: {result.timestamp}")
    print("-" * 60)
    time.sleep(0.5)

producer.flush()
producer.close()
print("\n[SUCCESS] All messages sent successfully!")