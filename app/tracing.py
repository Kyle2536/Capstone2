# app/tracing.py
import uuid
from datetime import datetime, timezone

def now_mysql_ms() -> str:
    """UTC timestamp formatted for MySQL DATETIME(3)"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def new_trace() -> dict:
    """Unique trace for one record + sensor timestamp (UTC)"""
    return {"trace_id": str(uuid.uuid4()), "sensor_created_at": now_mysql_ms()}