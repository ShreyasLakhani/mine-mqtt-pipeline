# mine-mqtt-pipeline

A small portfolio project demonstrating an end-to-end MQTT data pipeline. Three simulated sensors publish to a Mosquitto broker, a FastAPI consumer ingests and stores readings in PostgreSQL, and a REST API exposes the data. Built to practice MQTT protocol patterns and on-premise service orchestration with Docker Compose.

## Architecture

```
┌────────────┐   MQTT (QoS 1)    ┌────────────┐
│            │   port 1883       │            │
│  publisher ├──────────────────►│ mosquitto  │
│  (Python)  │   mine/sensors/#  │  (broker)  │
│            │                   │            │
└────────────┘                   └──────┬─────┘
                                        │
                                        │ subscribe mine/sensors/#
                                        ▼
                                 ┌────────────┐       ┌────────────┐
                                 │            │       │            │
                                 │  consumer  ├──────►│  postgres  │
                                 │  (FastAPI) │  SQL  │  (PG 16)   │
                                 │  :8000     │       │  :5432     │
                                 └────────────┘       └────────────┘
```

All four services run on a Docker bridge network (`mine-net`). The publisher and consumer resolve the broker and database by Docker hostname.

## MQTT Topics & Payload Schema

| Topic | Sensor Type | Unit | Normal Range | Alarm Threshold |
|---|---|---|---|---|
| `mine/sensors/temperature` | Ambient Temp | °C | 15 to 35 | none |
| `mine/sensors/methane` | Methane Gas | % LEL | 0 to 20 | > 25 % LEL |
| `mine/sensors/vibration` | Fan Vibration | mm/s RMS | 0 to 10 | none |

### Sample JSON Payload
Each reading is published as a structured JSON message:

```json
{
  "sensor_id": "methane",
  "value": 28.34,
  "unit": "% LEL",
  "timestamp": "2025-01-15T08:30:00.123456+00:00"
}
```

The publisher injects alarm-level methane readings roughly 15% of the time to produce realistic telemetry variety.

---

## Service Components

* **Publisher**: A Python script simulating three sensor nodes. Every two seconds it generates a reading and publishes over MQTT.
* **Mosquitto Broker**: Eclipse Mosquitto running on port 1883.
* **Consumer**: A background thread inside the FastAPI container. Subscribes to the sensor topics, evaluates readings against alarm thresholds, and writes to PostgreSQL.
* **Database**: PostgreSQL 16 stores historical readings.
* **REST API**: FastAPI exposes endpoints for health, latest readings, and alarm history.

---

## Quickstart

### Prerequisites
Docker and Docker Compose.

### Launch the Pipeline

```bash
git clone <repo-url> && cd mine-mqtt-pipeline
docker compose up --build
```

Wait about 10 seconds for PostgreSQL and the services to pass their health checks.

### Verify and Query the Live System
Open a separate terminal:

```bash
# Health check (DB + MQTT connectivity, uptime)
curl localhost:8000/health
# {"db":true,"mqtt":true,"uptime_seconds":42}

# Latest temperature readings
curl "localhost:8000/readings/latest?sensor_id=temperature&limit=3"
# [{"id":12,"sensor_id":"temperature","value":27.4,"unit":"°C","is_alarm":false,...}]

# General telemetry feed (default limit 50)
curl localhost:8000/readings/latest

# Active alarm readings (methane > 25 % LEL)
curl localhost:8000/readings/alarms
# [{"id":7,"sensor_id":"methane","value":36.71,"is_alarm":true,...}]
```

Interactive API docs are available at `http://localhost:8000/docs` (FastAPI Swagger UI).

### Stop the Pipeline

```bash
docker compose down -v
```

The `-v` flag also removes the PostgreSQL volume.

---

## Design Decisions

### MQTT QoS 1
QoS 1 (at-least-once delivery) was chosen over QoS 0 because sensor readings should not be silently dropped, and over QoS 2 because the four-way handshake is overkill for this workload. The database accepts duplicate rows without error, so at-least-once is a safe trade.

### Thread-Safe Database Pool
The MQTT callback runs on paho's background network thread, while FastAPI request handlers run on the main event loop's worker pool. Both write to PostgreSQL. A `psycopg2.pool.ThreadedConnectionPool` (1 to 5 connections) prevents collisions and avoids the per-message connection overhead.

### PostgreSQL, Not a Time-Series DB
For three sensors at 0.5 Hz, a dedicated time-series database (InfluxDB, TimescaleDB) adds operational weight without much benefit at this scale. A composite index on `(sensor_id, recorded_at DESC)` supports the main query pattern (latest N readings by sensor).

### Reconnect Strategy
Both publisher and consumer use paho's built-in reconnect (`reconnect_delay_set(min=1, max=30)`) and a manual retry loop on initial connection (up to 10 attempts, 2 seconds apart). This handles the common startup race where the broker container is not ready when the client containers start.

---

## Out of Scope

The following were deliberately left out to keep the demo focused:

* **Security**: Anonymous broker access, no TLS. Acceptable because all traffic stays inside the Docker bridge network.
* **Database Migrations**: Schema is created from a single SQL file on startup. No Alembic.
* **Frontend**: Backend only. The REST API returns JSON.
* **Testing**: Not included. Integration testing is via the documented curl commands. A production version would add pytest coverage for `mqtt_handler._on_message` payload parsing and `db.insert_reading` SQL.

---

## Running Locally Outside Docker (Development Mode)

For iterating on code without rebuilding containers, run the infrastructure in Docker and the Python services on the host:

### 1. Start broker and database only

```bash
docker compose up mosquitto postgres -d
```

### 2. Run the consumer on the host

```bash
cd consumer
python -m venv .venv
source .venv/bin/activate         # Windows: .venv\Scripts\activate
pip install -r requirements.txt

export MQTT_HOST=localhost        # Windows PowerShell: $env:MQTT_HOST="localhost"
export DB_HOST=localhost

uvicorn main:app --reload --port 8000
```

### 3. Run the publisher on the host

```bash
cd publisher
pip install -r requirements.txt

export MQTT_HOST=localhost
python publisher.py
```

---

## Directory Structure

```
mine-mqtt-pipeline/
├── docker-compose.yml          # 4-service orchestration
├── mosquitto/
│   └── config/mosquitto.conf   # Broker config (anonymous, no TLS)
├── publisher/
│   ├── Dockerfile
│   ├── requirements.txt        # paho-mqtt 2.1
│   └── publisher.py            # Sensor simulation + MQTT publish
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt        # FastAPI, paho-mqtt 2.1, psycopg2
│   ├── main.py                 # FastAPI app + lifespan
│   ├── db.py                   # PostgreSQL connection pool + queries
│   ├── mqtt_handler.py         # MQTT subscribe + alarm detection
│   └── schema.sql              # Table + index creation
├── .env.example                # Default environment variables
├── .gitignore
└── README.md
```