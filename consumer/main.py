"""FastAPI consumer service for mining sensor data.

Subscribes to MQTT sensor topics on startup, persists readings to
PostgreSQL, and exposes REST endpoints for querying stored data.
"""

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

import db
import mqtt_handler

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MQTT_HOST: str = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT: int = int(os.getenv("MQTT_PORT", "1883"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("consumer")

_start_time: float = 0.0


# ---------------------------------------------------------------------------
# Application lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _start_time
    _start_time = time.time()

    # Initialize database pool and schema
    db.init_pool()
    db.init_schema()

    # Start MQTT subscription (runs in background thread)
    mqtt_handler.start(MQTT_HOST, MQTT_PORT)
    logger.info("event=startup_complete")

    yield

    # Graceful shutdown
    mqtt_handler.stop()
    db.close_pool()
    logger.info("event=shutdown_complete")


app = FastAPI(title="Mine MQTT Consumer", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
def health() -> dict[str, Any]:
    """Service health including DB connectivity, MQTT status, and uptime."""
    return {
        "db": db.check_health(),
        "mqtt": mqtt_handler.is_connected(),
        "uptime_seconds": int(time.time() - _start_time),
    }


@app.get("/readings/latest")
def readings_latest(
    sensor_id: Optional[str] = Query(None, description="Filter by sensor ID"),
    limit: int = Query(50, ge=1, le=1000, description="Max rows to return"),
) -> list[dict[str, Any]]:
    """Return the most recent sensor readings."""
    return db.get_latest_readings(sensor_id=sensor_id, limit=limit)


@app.get("/readings/alarms")
def readings_alarms(
    limit: int = Query(50, ge=1, le=1000, description="Max rows to return"),
) -> list[dict[str, Any]]:
    """Return the most recent alarm-state readings (methane > 25 % LEL)."""
    return db.get_alarm_readings(limit=limit)
