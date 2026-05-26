"""MQTT publisher simulating underground mining sensors.

Publishes temperature, methane, and vibration readings every N seconds
to the Mosquitto broker. Methane readings occasionally exceed the 25 % LEL
alarm threshold to produce realistic data.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

# ---------------------------------------------------------------------------
# Configuration (all via environment variables)
# ---------------------------------------------------------------------------
MQTT_HOST: str = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT: int = int(os.getenv("MQTT_PORT", "1883"))
PUBLISH_INTERVAL: float = float(os.getenv("PUBLISH_INTERVAL", "2.0"))
ALARM_PROBABILITY: float = float(os.getenv("ALARM_PROBABILITY", "0.15"))

# ---------------------------------------------------------------------------
# Sensor definitions
# ---------------------------------------------------------------------------
SENSORS: list[dict[str, Any]] = [
    {
        "sensor_id": "temperature",
        "topic": "mine/sensors/temperature",
        "unit": "°C",
        "range": (15.0, 45.0),
        "normal_max": 35.0,
    },
    {
        "sensor_id": "methane",
        "topic": "mine/sensors/methane",
        "unit": "% LEL",
        "range": (0.0, 100.0),
        "normal_max": 20.0,
    },
    {
        "sensor_id": "vibration",
        "topic": "mine/sensors/vibration",
        "unit": "mm/s RMS",
        "range": (0.0, 20.0),
        "normal_max": 10.0,
    },
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("publisher")

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_shutdown: bool = False


def _handle_signal(signum: int, _frame: Any) -> None:
    global _shutdown
    logger.info("signal=%s action=shutdown_requested", signal.Signals(signum).name)
    _shutdown = True


# ---------------------------------------------------------------------------
# MQTT callbacks  (paho-mqtt v2 callback API)
# ---------------------------------------------------------------------------
def on_connect(
    client: mqtt.Client,
    userdata: Any,
    flags: Any,
    reason_code: Any,
    properties: Any,
) -> None:
    if reason_code == 0:
        logger.info("event=connected broker=%s:%d", MQTT_HOST, MQTT_PORT)
    else:
        logger.error("event=connect_failed reason=%s", reason_code)


def on_disconnect(
    client: mqtt.Client,
    userdata: Any,
    flags: Any,
    reason_code: Any,
    properties: Any,
) -> None:
    logger.warning("event=disconnected reason=%s", reason_code)


# ---------------------------------------------------------------------------
# Sensor simulation
# ---------------------------------------------------------------------------
def generate_reading(sensor: dict[str, Any]) -> dict[str, Any]:
    """Generate a simulated sensor reading.

    Methane has a configurable probability of producing alarm-level values
    (> 25 % LEL) to make the data stream realistic.
    """
    sensor_id: str = sensor["sensor_id"]
    low, high = sensor["range"]

    if sensor_id == "methane" and random.random() < ALARM_PROBABILITY:
        # Alarm condition — above 25 % LEL threshold
        value = round(random.uniform(26.0, high), 2)
    else:
        # Normal operating range
        value = round(random.uniform(low, sensor["normal_max"]), 2)

    return {
        "sensor_id": sensor_id,
        "value": value,
        "unit": sensor["unit"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="mine-publisher",
    )
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    # Retry initial connection — broker may still be starting
    for attempt in range(1, 11):
        try:
            client.connect(MQTT_HOST, MQTT_PORT)
            break
        except (ConnectionRefusedError, OSError) as exc:
            logger.warning(
                "event=connect_retry attempt=%d error=%s", attempt, exc
            )
            time.sleep(2)
    else:
        logger.critical(
            "event=connect_failed_permanently broker=%s:%d",
            MQTT_HOST,
            MQTT_PORT,
        )
        sys.exit(1)

    client.loop_start()
    logger.info(
        "event=publishing_started interval=%.1fs sensors=%d",
        PUBLISH_INTERVAL,
        len(SENSORS),
    )

    try:
        while not _shutdown:
            for sensor in SENSORS:
                reading = generate_reading(sensor)
                payload = json.dumps(reading)
                client.publish(sensor["topic"], payload, qos=1)
                logger.info(
                    "event=published topic=%s sensor_id=%s value=%.2f",
                    sensor["topic"],
                    reading["sensor_id"],
                    reading["value"],
                )
            time.sleep(PUBLISH_INTERVAL)
    finally:
        client.loop_stop()
        client.disconnect()
        logger.info("event=shutdown_complete")


if __name__ == "__main__":
    main()
