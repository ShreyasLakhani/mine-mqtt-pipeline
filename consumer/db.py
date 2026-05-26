"""PostgreSQL access layer for sensor readings.

Uses a ThreadedConnectionPool so the MQTT callback thread and the
FastAPI request threads can safely share connections.
"""

import logging
import os
import time
from pathlib import Path
from typing import Any, Optional

import psycopg2
from psycopg2 import pool as pg_pool

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_HOST: str = os.getenv("DB_HOST", "postgres")
DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
DB_NAME: str = os.getenv("DB_NAME", "mine")
DB_USER: str = os.getenv("DB_USER", "mine")
DB_PASSWORD: str = os.getenv("DB_PASSWORD", "mine")

logger = logging.getLogger("consumer.db")

_pool: Optional[pg_pool.ThreadedConnectionPool] = None


# ---------------------------------------------------------------------------
# Pool lifecycle
# ---------------------------------------------------------------------------
def init_pool(max_retries: int = 10) -> None:
    """Create the connection pool, retrying if Postgres isn't ready yet."""
    global _pool
    for attempt in range(1, max_retries + 1):
        try:
            _pool = pg_pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=5,
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )
            logger.info("event=pool_initialized host=%s db=%s", DB_HOST, DB_NAME)
            return
        except psycopg2.OperationalError as exc:
            logger.warning(
                "event=db_connect_retry attempt=%d/%d error=%s",
                attempt, max_retries, exc,
            )
            if attempt < max_retries:
                time.sleep(2)
    raise ConnectionError(
        f"Failed to connect to {DB_HOST}:{DB_PORT}/{DB_NAME} "
        f"after {max_retries} attempts"
    )


def init_schema() -> None:
    """Run schema.sql to create tables and indexes (idempotent)."""
    schema_path = Path(__file__).parent / "schema.sql"
    sql = schema_path.read_text()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info("event=schema_initialized")
    finally:
        _put_conn(conn)


def close_pool() -> None:
    """Shut down all connections in the pool."""
    if _pool is not None:
        _pool.closeall()
        logger.info("event=pool_closed")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _get_conn() -> Any:
    assert _pool is not None, "Connection pool not initialized"
    return _pool.getconn()


def _put_conn(conn: Any) -> None:
    assert _pool is not None, "Connection pool not initialized"
    _pool.putconn(conn)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
def check_health() -> bool:
    """Return True if we can execute a trivial query."""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        finally:
            _put_conn(conn)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------
def insert_reading(
    *,
    sensor_id: str,
    topic: str,
    value: float,
    unit: str,
    is_alarm: bool,
    recorded_at: str,
) -> None:
    """Insert a single sensor reading."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sensor_readings
                    (sensor_id, topic, value, unit, is_alarm, recorded_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (sensor_id, topic, value, unit, is_alarm, recorded_at),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------
def get_latest_readings(
    sensor_id: Optional[str] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Return the most recent readings, optionally filtered by sensor_id."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            if sensor_id:
                cur.execute(
                    """
                    SELECT id, sensor_id, topic, value, unit,
                           is_alarm, recorded_at, received_at
                      FROM sensor_readings
                     WHERE sensor_id = %s
                     ORDER BY recorded_at DESC
                     LIMIT %s
                    """,
                    (sensor_id, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT id, sensor_id, topic, value, unit,
                           is_alarm, recorded_at, received_at
                      FROM sensor_readings
                     ORDER BY recorded_at DESC
                     LIMIT %s
                    """,
                    (limit,),
                )
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        _put_conn(conn)


def get_alarm_readings(limit: int = 50) -> list[dict[str, Any]]:
    """Return the most recent alarm-state readings."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, sensor_id, topic, value, unit,
                       is_alarm, recorded_at, received_at
                  FROM sensor_readings
                 WHERE is_alarm = TRUE
                 ORDER BY recorded_at DESC
                 LIMIT %s
                """,
                (limit,),
            )
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        _put_conn(conn)
