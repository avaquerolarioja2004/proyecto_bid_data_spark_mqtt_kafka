CREATE TABLE IF NOT EXISTS devices (
    device_id VARCHAR(64) PRIMARY KEY,
    city_id VARCHAR(64) NOT NULL,
    district VARCHAR(100) NOT NULL,
    zone_type VARCHAR(50) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    installed_at TIMESTAMPTZ DEFAULT NOW(),
    firmware_version VARCHAR(32),
    status VARCHAR(20) DEFAULT 'ONLINE'
);

CREATE TABLE IF NOT EXISTS latest_measurements (
    device_id VARCHAR(64) PRIMARY KEY REFERENCES devices(device_id),
    event_timestamp TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    humidity DOUBLE PRECISION,
    surface_temperature DOUBLE PRECISION,
    solar_radiation DOUBLE PRECISION,
    battery_level DOUBLE PRECISION,
    signal_strength INTEGER
);

CREATE TABLE IF NOT EXISTS hourly_metrics (
    bucket_start TIMESTAMPTZ NOT NULL,
    district VARCHAR(100) NOT NULL,
    zone_type VARCHAR(50) NOT NULL,
    avg_temperature DOUBLE PRECISION,
    max_temperature DOUBLE PRECISION,
    min_temperature DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    measurement_count BIGINT,
    PRIMARY KEY (bucket_start, district, zone_type)
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id VARCHAR(100) PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    device_id VARCHAR(64),
    district VARCHAR(100),
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    temperature DOUBLE PRECISION,
    message TEXT
);

CREATE INDEX IF NOT EXISTS idx_hourly_metrics_bucket ON hourly_metrics(bucket_start);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at);

INSERT INTO devices(device_id, city_id, district, zone_type, latitude, longitude, firmware_version)
VALUES
('LOG-0001','logrono','Centro','CITY_CENTER',42.4658,-2.4498,'1.4.2'),
('LOG-0002','logrono','Parque del Carmen','PARK',42.4639,-2.4472,'1.4.2'),
('LOG-0003','logrono','Varea','INDUSTRIAL',42.4477,-2.4034,'1.4.2'),
('LOG-0004','logrono','San Adrián','RESIDENTIAL',42.4567,-2.4661,'1.4.2'),
('LOG-0005','logrono','Las Gaunas','TRANSPORT',42.4541,-2.4490,'1.4.2')
ON CONFLICT DO NOTHING;
