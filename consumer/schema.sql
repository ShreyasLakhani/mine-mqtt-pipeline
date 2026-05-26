CREATE TABLE IF NOT EXISTS sensor_readings (
    id          BIGSERIAL       PRIMARY KEY,
    sensor_id   TEXT            NOT NULL,
    topic       TEXT            NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    unit        TEXT            NOT NULL,
    is_alarm    BOOLEAN         NOT NULL DEFAULT FALSE,
    recorded_at TIMESTAMPTZ     NOT NULL,
    received_at TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_readings_sensor_time
    ON sensor_readings (sensor_id, recorded_at DESC);
