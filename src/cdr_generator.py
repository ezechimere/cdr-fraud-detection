from kafka import KafkaProducer
import json
from datetime import datetime
from faker import Faker
import random
import time
import sys

fake = Faker()

class CDRGenerator:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8')
        )
        
        # Nigerian phone numbers pool (simulate 1000 users)
        self.msisdns = [f"234{random.choice(['080', '081', '070', '090'])}{random.randint(10000000, 99999999)}" 
                       for _ in range(1000)]
        
        # Lagos cell towers
        self.cell_towers = [
            'LAG_VI_001', 'LAG_VI_002', 'LAG_LEKKI_001', 'LAG_LEKKI_002',
            'LAG_IKEJA_001', 'LAG_IKEJA_002', 'LAG_SURULERE_001', 'LAG_YABA_001'
        ]
        
    def generate_voice_cdr(self):
        """Generate voice call CDR"""
        return {
            'record_type': 'voice',
            'msisdn': random.choice(self.msisdns),
            'destination': random.choice(self.msisdns),
            'call_duration': random.randint(10, 3600),
            'call_type': random.choice(['MOC', 'MTC']),
            'cell_id': random.choice(self.cell_towers),
            'call_status': random.choice(['completed', 'busy', 'no_answer', 'failed']),
            'timestamp': datetime.now().isoformat(),
            'revenue': round(random.uniform(5, 50), 2)
        }
    
    def generate_sms_cdr(self):
        """Generate SMS CDR"""
        return {
            'record_type': 'sms',
            'msisdn': random.choice(self.msisdns),
            'destination': random.choice(self.msisdns),
            'sms_type': random.choice(['MO', 'MT']),
            'cell_id': random.choice(self.cell_towers),
            'status': random.choice(['delivered', 'failed']),
            'timestamp': datetime.now().isoformat(),
            'revenue': 4.0
        }
    
    def generate_data_cdr(self):
        """Generate data session CDR"""
        return {
            'record_type': 'data',
            'msisdn': random.choice(self.msisdns),
            'bytes_uploaded': random.randint(1024, 10485760),
            'bytes_downloaded': random.randint(10240, 104857600),
            'session_duration': random.randint(60, 7200),
            'cell_id': random.choice(self.cell_towers),
            'apn': random.choice(['internet', 'wap', 'mms']),
            'timestamp': datetime.now().isoformat(),
            'revenue': round(random.uniform(10, 200), 2)
        }
    
    def generate_stream(self, events_per_second=10, duration_seconds=60):
        """Generate continuous stream of CDR events"""
        print(f"Starting CDR stream: {events_per_second} events/sec for {duration_seconds} seconds\n", flush=True)
        sys.stdout.flush()
        
        event_count = 0
        start_time = time.time()
        
        while (time.time() - start_time) < duration_seconds:
            # Random CDR type (60% voice, 20% SMS, 20% data)
            rand = random.random()
            if rand < 0.6:
                cdr = self.generate_voice_cdr()
            elif rand < 0.8:
                cdr = self.generate_sms_cdr()
            else:
                cdr = self.generate_data_cdr()
            
            # Send to Kafka
            self.producer.send(
                topic='cdr-events',
                key=cdr['msisdn'],
                value=cdr
            )
            
            event_count += 1
            
            if event_count % 100 == 0:
                print(f"[OK] Sent {event_count} events...", flush=True)
                sys.stdout.flush()
            
            # Control rate
            time.sleep(1.0 / events_per_second)
        
        self.producer.flush()
        actual_duration = time.time() - start_time
        print(f"\n[COMPLETE] Stream finished!", flush=True)
        print(f"  Total events: {event_count}", flush=True)
        print(f"  Duration: {actual_duration:.2f}s", flush=True)
        print(f"  Actual rate: {event_count/actual_duration:.2f} events/sec", flush=True)
        sys.stdout.flush()
    
    def close(self):
        self.producer.close()

if __name__ == "__main__":
    generator = CDRGenerator()
    
    try:
        # Generate 600 events (10/sec for 60 seconds)
        generator.generate_stream(events_per_second=10, duration_seconds=60)
    except KeyboardInterrupt:
        print("\n\nStream interrupted", flush=True)
    finally:
        generator.close()