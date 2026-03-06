import psycopg2
from tabulate import tabulate

def query_database():
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='cdr_analytics',
        user='kafka_user',
        password='kafka_pass'
    )
    
    cursor = conn.cursor()
    
    print("\n=== TABLES ===")
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    for row in cursor.fetchall():
        print(f"  - {row[0]}")
    
    print("\n=== RECORD COUNTS ===")
    tables = ['cdr_raw', 'voice_calls', 'sms_records', 'data_sessions', 'cdr_alerts', 'realtime_metrics']
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count} records")
    
    print("\n=== RECENT VOICE CALLS ===")
    cursor.execute("""
        SELECT msisdn, destination, duration, revenue, call_timestamp 
        FROM voice_calls 
        ORDER BY call_timestamp DESC 
        LIMIT 5
    """)
    rows = cursor.fetchall()
    if rows:
        headers = ['MSISDN', 'Destination', 'Duration', 'Revenue', 'Timestamp']
        print(tabulate(rows, headers=headers, tablefmt='grid'))
    else:
        print("  No voice calls yet")
    
    print("\n=== REAL-TIME METRICS ===")
    cursor.execute("SELECT metric_key, metric_value, updated_at FROM realtime_metrics")
    rows = cursor.fetchall()
    if rows:
        headers = ['Metric', 'Value', 'Updated']
        print(tabulate(rows, headers=headers, tablefmt='grid'))
    else:
        print("  No metrics yet")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    query_database()