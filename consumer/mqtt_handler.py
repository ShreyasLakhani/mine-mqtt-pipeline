"""MQTT subscription handler for mining sensor data.

Subscribes to mine/sensors/# and persists every message to PostgreSQL.
Alarm detection is done here based on configurable thresholds.
"""

import json
import logging
import time
from typing import Any, Optional

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from db import insert_reading

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SUBSCRIBE_TOPIC: str = "mine/sensors/#"

ALARM_THRESHOLDS: dict[str, float] = {
    "methane": 25.0,  # % LEL
}

logger = logging.getLogger("consumer.mqtt")

_client: Optional[mqtt.Client] = None
_connected: bool = False


# ---------------------------------------------------------------------------
# Public state
# ---------------------------------------------------------------------------
def is_connected() -> bool:
    """Return True if the MQTT client is currently connected."""
    return _connected


# ---------------------------------------------------------------------------
# Callbacks  (paho-mqtt v2 callback API)
# ---------------------------------------------------------------------------
def _on_connect(
    client: mqtt.Client,
    userdata: Any,
    flags: Any,
    reason_code: Any,
    properties: Any,
) -> None:
    global _connected
    if reason_code == 0:
        _connected = True
        client.subscribe(SUBSCRIBE_TOPIC, qos=1)
        logger.info("event=connected subscribed_to=%s", SUBSCRIBE_TOPIC)
    else:
        _connected = False
        logger.error("event=connect_failed reason=%s", reason_code)


def _on_disconnect(
    client: mqtt.Client,
    userdata: Any,
    flags: Any,
    reason_code: Any,
    properties: Any,
) -> None:
    global _connected
    _connected = False
    logger.warning("event=disconnected reason=%s", reason_code)


def _on_message(
    client: mqtt.Client,
    userdata: Any,
    message: mqtt.MQTTMessage,
) -> None:
    """Parse incoming sensor payload, detect alarms, persist to DB."""
    try:
        payload: dict[str, Any] = json.loads(message.payload.decode())
        sensor_id: str = payload["sensor_id"]
        value: float = float(payload["value"])
        unit: str = payload["unit"]
        recorded_at: str = payload["timestamp"]
        topic: str = message.topic

        is_alarm: bool = (
            sensor_id in ALARM_THRESHOLDS
            and value > ALARM_THRESHOLDS[sensor_id]
        )

        if is_alarm:
            logger.warning(
                "event=alarm sensor_id=%s value=%.2f threshold=%.2f",
                sensor_id,
                value,
                ALARM_THRESHOLDS[sensor_id],
            )

        insert_reading(
            sensor_id=sensor_id,
            topic=topic,
            value=value,
            unit=unit,
            is_alarm=is_alarm,
            recorded_at=recorded_at,
        )
        logger.info(
            "event=reading_stored sensor_id=%s value=%.2f alarm=%s",
            sensor_id,
            value,
            is_alarm,
        )

    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        logger.error(
            "event=message_parse_error topic=%s error=%s", message.topic, exc
        )
    except Exception as exc:
        logger.error(
            "event=message_processing_error topic=%s error=%s",
            message.topic,
            exc,
        )


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------
def start(mqtt_host: str, mqtt_port: int) -> mqtt.Client:
    """Connect to the broker and start the network loop thread."""
    global _client
    _client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="mine-consumer",
    )
    _client.on_connect = _on_connect
    _client.on_disconnect = _on_disconnect
    _client.on_message = _on_message
    _client.reconnect_delay_set(min_delay=1, max_delay=30)

    # Retry initial connection — broker may still be starting
    for attempt in range(1, 11):
        try:
            _client.connect(mqtt_host, mqtt_port)
            break
        except (ConnectionRefusedError, OSError) as exc:
            logger.warning(
                "event=connect_retry attempt=%d error=%s", attempt, exc
            )
            time.sleep(2)
    else:
        raise ConnectionError(
            f"Failed to connect to MQTT broker at {mqtt_host}:{mqtt_port}"
        )

    _client.loop_start()
    return _client


def stop() -> None:
    """Disconnect and stop the network loop thread."""
    global _client, _connected
    if _client is not None:
        _client.loop_stop()
        _client.disconnect()
        _connected = False
        logger.info("event=mqtt_stopped")
