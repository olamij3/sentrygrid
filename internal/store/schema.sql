CREATE TABLE IF NOT EXISTS readings (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT     NOT NULL,
    timestamp DATETIME NOT NULL,
    value     REAL     NOT NULL
);

CREATE TABLE IF NOT EXISTS anomalies (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT     NOT NULL,
    timestamp DATETIME NOT NULL,
    value     REAL     NOT NULL,
    method    TEXT     NOT NULL,
    score     REAL     NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_readings_device ON readings(device_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_anomalies_device ON anomalies(device_id, timestamp);