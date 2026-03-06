-- CDR Analytics Database Schema

-- Raw CDR events table (for archival)
CREATE TABLE IF NOT EXISTS cdr_raw (
    id BIGSERIAL PRIMARY KEY,
    msisdn VARCHAR(15) NOT NULL,
    record_type VARCHAR(10) NOT NULL,
    event_data JSONB NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    kafka_partition INT,
    kafka_offset BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdr_raw_msisdn ON cdr_raw(msisdn);
CREATE INDEX idx_cdr_raw_timestamp ON cdr_raw(timestamp);
CREATE INDEX idx_cdr_raw_type ON cdr_raw(record_type);

-- Voice calls aggregated
CREATE TABLE IF NOT EXISTS voice_calls (
    id BIGSERIAL PRIMARY KEY,
    msisdn VARCHAR(15) NOT NULL,
    destination VARCHAR(15),
    duration INT NOT NULL,
    call_type VARCHAR(3),
    cell_id VARCHAR(50),
    status VARCHAR(20),
    revenue DECIMAL(10,2),
    call_timestamp TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_voice_msisdn ON voice_calls(msisdn);
CREATE INDEX idx_voice_timestamp ON voice_calls(call_timestamp);

-- SMS aggregated
CREATE TABLE IF NOT EXISTS sms_records (
    id BIGSERIAL PRIMARY KEY,
    msisdn VARCHAR(15) NOT NULL,
    destination VARCHAR(15),
    sms_type VARCHAR(2),
    cell_id VARCHAR(50),
    status VARCHAR(20),
    revenue DECIMAL(10,2),
    sms_timestamp TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_sms_msisdn ON sms_records(msisdn);

-- Data sessions aggregated
CREATE TABLE IF NOT EXISTS data_sessions (
    id BIGSERIAL PRIMARY KEY,
    msisdn VARCHAR(15) NOT NULL,
    bytes_uploaded BIGINT,
    bytes_downloaded BIGINT,
    total_bytes BIGINT GENERATED ALWAYS AS (bytes_uploaded + bytes_downloaded) STORED,
    session_duration INT,
    cell_id VARCHAR(50),
    apn VARCHAR(50),
    revenue DECIMAL(10,2),
    session_timestamp TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_data_msisdn ON data_sessions(msisdn);

-- Real-time metrics (updated continuously)
CREATE TABLE IF NOT EXISTS realtime_metrics (
    metric_key VARCHAR(100) PRIMARY KEY,
    metric_value NUMERIC,
    metric_data JSONB,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alerts (fraud detection, anomalies)
CREATE TABLE IF NOT EXISTS cdr_alerts (
    id BIGSERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,
    msisdn VARCHAR(15),
    severity VARCHAR(10),
    description TEXT,
    alert_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_alerts_type ON cdr_alerts(alert_type);
CREATE INDEX idx_alerts_msisdn ON cdr_alerts(msisdn);
CREATE INDEX idx_alerts_severity ON cdr_alerts(severity);