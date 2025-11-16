# --- robust shim for environments where kafka.vendor.six is missing ---
import sys
try:
    import six  # provided by requirements.txt
    # Map kafka.vendor.six to the real six module (exposes PY3, string_types, moves, etc.)
    sys.modules['kafka.vendor.six'] = six
    sys.modules['kafka.vendor.six.moves'] = six.moves
except Exception:
    pass
# ----------------------------------------------------------------------


import os
import json
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from generator_fixed import generate_records  # Option B generator

# --- Env ---
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "traffic.raw.v2")
NUM_RECORDS = int(os.getenv("NUM_RECORDS", "1000"))
LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", "10"))
COMPRESSION = os.getenv("KAFKA_COMPRESSION", "lz4")
ACKS = os.getenv("KAFKA_ACKS", "all")

RUN_ID = os.getenv("RUN_ID") or datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%S")

def _to_db_schema(rec: dict) -> dict:
    """
    Generator yields:
      created_at (YYYY-MM-DD), timestamp (HH:MM:SS.mmm), peakspeed, pmgid, direction, location, vehiclecount
      + trace_id, sensor_created_at (UTC, ms) via new_trace()
    DB expects 'ts' instead of 'timestamp' and needs run_id included.
    """
    out = rec.copy()
    out["ts"] = out.pop("timestamp")
    out["run_id"] = RUN_ID
    return out

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks=ACKS,
        compression_type=COMPRESSION,
        linger_ms=LINGER_MS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        retries=5,
        max_in_flight_requests_per_connection=5,  # plays nicely with acks='all'
        api_version_auto_timeout_ms=60000,
    )

    sent = 0
    for rec in generate_records(NUM_RECORDS):
        payload = _to_db_schema(rec)
        key = payload["pmgid"]  # stable partitioning by sensor
        producer.send(TOPIC, key=key, value=payload)
        sent += 1

    producer.flush()
    print(f"[producer] Sent {sent} records to topic '{TOPIC}' with RUN_ID={RUN_ID}")

if __name__ == "__main__":
    main()
