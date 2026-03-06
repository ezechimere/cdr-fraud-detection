import psycopg2
from psycopg2.extras import execute_batch, Json
from contextlib import contextmanager
import sys
import socket

print(socket.getaddrinfo('localhost', 5432))
class DatabaseManager:
    def __init__(self):
        self.conn_params = {
            'host': '127.0.0.1', # IPv4 instead of 'localhost'
            'port': 5432,
            'database': 'cdr_analytics',
            'user': 'kafka_user',
            'password': 'kafka_pass'
        }
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = psycopg2.connect(**self.conn_params)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def insert_raw_cdr(self, records):
        """Insert raw CDR records"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            query = """
                INSERT INTO cdr_raw (msisdn, record_type, event_data, timestamp, kafka_partition, kafka_offset)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            execute_batch(cursor, query, records)
            cursor.close()
    
    def insert_voice_call(self, call_data):
        """Insert processed voice call"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            query = """
                INSERT INTO voice_calls 
                (msisdn, destination, duration, call_type, cell_id, status, revenue, call_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                call_data['msisdn'],
                call_data.get('destination'),
                call_data['call_duration'],
                call_data['call_type'],
                call_data['cell_id'],
                call_data['call_status'],
                call_data['revenue'],
                call_data['timestamp']
            ))
            cursor.close()
    
    def insert_sms_record(self, sms_data):
        """Insert SMS record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            query = """
                INSERT INTO sms_records 
                (msisdn, destination, sms_type, cell_id, status, revenue, sms_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                sms_data['msisdn'],
                sms_data.get('destination'),
                sms_data['sms_type'],
                sms_data['cell_id'],
                sms_data['status'],
                sms_data['revenue'],
                sms_data['timestamp']
            ))
            cursor.close()
    
    def insert_data_session(self, data_session):
        """Insert data session record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            query = """
                INSERT INTO data_sessions 
                (msisdn, bytes_uploaded, bytes_downloaded, session_duration, cell_id, apn, revenue, session_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                data_session['msisdn'],
                data_session['bytes_uploaded'],
                data_session['bytes_downloaded'],
                data_session['session_duration'],
                data_session['cell_id'],
                data_session['apn'],
                data_session['revenue'],
                data_session['timestamp']
            ))
            cursor.close()
    
    def update_realtime_metric(self, metric_key, metric_value, metric_data=None):
        """Update real-time metric"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            query = """
                INSERT INTO realtime_metrics (metric_key, metric_value, metric_data, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (metric_key) 
                DO UPDATE SET 
                    metric_value = EXCLUDED.metric_value,
                    metric_data = EXCLUDED.metric_data,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(query, (metric_key, metric_value, Json(metric_data) if metric_data else None))
            cursor.close()
    
    def insert_alert(self, alert_data):
        """Insert alert"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            query = """
                INSERT INTO cdr_alerts (alert_type, msisdn, severity, description, alert_data)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                alert_data['alert_type'],
                alert_data.get('msisdn'),
                alert_data.get('severity', 'medium'),
                alert_data.get('description'),
                Json(alert_data.get('data'))
            ))
            cursor.close()
    
    def get_metrics(self):
        """Fetch current metrics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM realtime_metrics")
            results = cursor.fetchall()
            cursor.close()
            return results

# Test connection
if __name__ == "__main__":
    db = DatabaseManager()
    print("[OK] Database connection successful!", flush=True)
    
    # Test metric update
    db.update_realtime_metric('test_metric', 100, {'status': 'ok'})
    print("[OK] Metric update successful!", flush=True)
    
    metrics = db.get_metrics()
    print(f"[OK] Current metrics: {len(metrics)}", flush=True)
    
    for metric in metrics:
        print(f"  - {metric[0]}: {metric[1]}", flush=True)