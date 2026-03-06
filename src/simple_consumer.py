from kafka import KafkaConsumer
import json
import sys

# Initialize consumer
consumer = KafkaConsumer(
    'cdr-events',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

print("Listening for CDR events... (Ctrl+C to stop)\n", flush=True)

try:
    for message in consumer:
        cdr = message.value
        record_type = cdr.get('record_type', 'unknown')
        
        print(f"MSISDN: {message.key}", flush=True)
        print(f"  Record Type: {record_type}", flush=True)
        print(f"  Partition: {message.partition} | Offset: {message.offset}", flush=True)
        
        # Display type-specific details
        if record_type == 'voice':
            print(f"  Call Duration: {cdr.get('call_duration')}s", flush=True)
            print(f"  Status: {cdr.get('call_status')}", flush=True)
            print(f"  Revenue: N{cdr.get('revenue')}", flush=True)
        elif record_type == 'sms':
            print(f"  SMS Type: {cdr.get('sms_type')}", flush=True)
            print(f"  Status: {cdr.get('status')}", flush=True)
            print(f"  Revenue: N{cdr.get('revenue')}", flush=True)
        elif record_type == 'data':
            bytes_down = cdr.get('bytes_downloaded', 0)
            bytes_up = cdr.get('bytes_uploaded', 0)
            total_mb = (bytes_down + bytes_up) / 1048576
            print(f"  Data Usage: {total_mb:.2f} MB", flush=True)
            print(f"  Session Duration: {cdr.get('session_duration')}s", flush=True)
            print(f"  Revenue: N{cdr.get('revenue')}", flush=True)
        
        print(f"  Cell: {cdr.get('cell_id')}", flush=True)
        print("-" * 60, flush=True)
        sys.stdout.flush()
        
except KeyboardInterrupt:
    print("\n\nConsumer stopped", flush=True)
finally:
    consumer.close()