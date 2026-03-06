from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime
from database import DatabaseManager
import redis
from collections import defaultdict
import time
import sys

class CDRConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'cdr-events',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='cdr-processing-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            max_poll_records=100
        )
        
        self.db = DatabaseManager()
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.metrics = defaultdict(int)
        self.last_flush = time.time()
        
        self.alert_producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
    def process_voice_cdr(self, cdr, partition, offset):
        """Process voice call CDR"""
        try:
            self.db.insert_voice_call(cdr)
            
            self.metrics['total_voice_calls'] += 1
            self.metrics['total_voice_duration'] += cdr.get('call_duration', 0)
            self.metrics['total_voice_revenue'] += cdr.get('revenue', 0)
            
            self.redis_client.incr('voice_calls_today')
            self.redis_client.incrbyfloat('voice_revenue_today', cdr.get('revenue', 0))
            
            # LOWERED THRESHOLDS FOR TESTING
            
            # Fraud detection: Long call (lowered from 7200 to 1800 = 30 min)
            if cdr.get('call_duration', 0) > 1800:
                self.send_alert({
                    'alert_type': 'long_duration_call',
                    'msisdn': cdr['msisdn'],
                    'severity': 'medium',
                    'description': f"Call duration {cdr['call_duration']}s exceeds 30 min threshold",
                    'data': cdr
                })
            
            # Fraud detection: High value call (lowered from 1000 to 40)
            if cdr.get('revenue', 0) > 40:
                self.send_alert({
                    'alert_type': 'high_revenue_call',
                    'msisdn': cdr['msisdn'],
                    'severity': 'high',
                    'description': f"Call revenue N{cdr['revenue']} exceeds N40 threshold",
                    'data': cdr
                })
                
        except Exception as e:
            print(f"[ERROR] Processing voice CDR: {e}", flush=True)
            raise
    
    def process_sms_cdr(self, cdr, partition, offset):
        """Process SMS CDR"""
        try:
            self.db.insert_sms_record(cdr)
            
            self.metrics['total_sms'] += 1
            self.metrics['total_sms_revenue'] += cdr.get('revenue', 0)
            
            self.redis_client.incr('sms_today')
            
        except Exception as e:
            print(f"[ERROR] Processing SMS CDR: {e}", flush=True)
            raise
    
    def process_data_cdr(self, cdr, partition, offset):
        """Process data session CDR"""
        try:
            total_bytes = cdr.get('bytes_uploaded', 0) + cdr.get('bytes_downloaded', 0)
            
            self.db.insert_data_session(cdr)
            
            self.metrics['total_data_sessions'] += 1
            self.metrics['total_bytes'] += total_bytes
            self.metrics['total_data_revenue'] += cdr.get('revenue', 0)
            
            self.redis_client.incr('data_sessions_today')
            self.redis_client.incrbyfloat('data_bytes_today', total_bytes)
            
            # Alert: Large data usage (lowered from 1GB to 50MB)
            if total_bytes > 52428800:  # > 50MB
                self.send_alert({
                    'alert_type': 'high_data_usage',
                    'msisdn': cdr['msisdn'],
                    'severity': 'low',
                    'description': f"Data usage {total_bytes/1048576:.2f}MB in single session",
                    'data': cdr
                })
                
        except Exception as e:
            print(f"[ERROR] Processing data CDR: {e}", flush=True)
            raise
    
    def send_alert(self, alert_data):
        """Send alert to alerts topic and store in database"""
        alert_data['timestamp'] = datetime.now().isoformat()
        
        self.alert_producer.send('cdr-alerts', value=alert_data)
        
        self.db.insert_alert(alert_data)
        
        print(f"[ALERT] {alert_data['alert_type']} - {alert_data['msisdn']}", flush=True)
        sys.stdout.flush()
    
    def flush_metrics(self):
        """Flush accumulated metrics to database"""
        if time.time() - self.last_flush < 10:
            return
        
        for metric_key, metric_value in self.metrics.items():
            self.db.update_realtime_metric(metric_key, metric_value)
        
        print(f"[METRICS] Flushed: {dict(self.metrics)}", flush=True)
        sys.stdout.flush()
        self.last_flush = time.time()
    
    def consume(self):
        """Main consumer loop"""
        print("CDR Consumer started. Processing events...\n", flush=True)
        sys.stdout.flush()
        
        try:
            batch_count = 0
            for message in self.consumer:
                cdr = message.value
                record_type = cdr.get('record_type')
                
                if record_type == 'voice':
                    self.process_voice_cdr(cdr, message.partition, message.offset)
                elif record_type == 'sms':
                    self.process_sms_cdr(cdr, message.partition, message.offset)
                elif record_type == 'data':
                    self.process_data_cdr(cdr, message.partition, message.offset)
                
                batch_count += 1
                
                if batch_count % 50 == 0:
                    self.consumer.commit()
                    self.flush_metrics()
                    print(f"[OK] Processed {batch_count} messages", flush=True)
                    sys.stdout.flush()
                
        except KeyboardInterrupt:
            print("\n\nConsumer stopped by user", flush=True)
        finally:
            self.flush_metrics()
            self.consumer.close()
            self.alert_producer.close()
            print("Consumer shut down gracefully", flush=True)

if __name__ == "__main__":
    consumer = CDRConsumer()
    consumer.consume()