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
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# Generator APIs:
# - generate_records(N): finite stream honoring your rush/late/normal logic
# - generate_burst_for_sensor(pmgid, base_dt_utc, cars_in_burst)
# - daypart_multipliers(local_dt) -> (speed_mult, volume_mult)  [for pacing only]
# - now_local_chicago() -> local dt (America/Chicago)
from generator_fixed import (
    generate_records,
    generate_burst_for_sensor,
    daypart_multipliers,
    now_local_chicago,
)

from dotenv import load_dotenv
load_dotenv()

# ----------------- Env -----------------
BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC       = os.getenv("KAFKA_TOPIC", "traffic.raw.v2")
NUM_RECORDS = int(os.getenv("NUM_RECORDS", "1000"))      # used when RUN_FOREVER=0
LINGER_MS   = int(os.getenv("KAFKA_LINGER_MS", "10"))
COMPRESSION = os.getenv("KAFKA_COMPRESSION", "lz4")
ACKS        = os.getenv("KAFKA_ACKS", "all")

# Continuous mode controls
RUN_FOREVER = os.getenv("RUN_FOREVER", "0") == "1"
BASE_SLEEP  = float(os.getenv("BASE_SLEEP", "0.20"))     # seconds between bursts at Normal
BURST_MIN   = int(os.getenv("BURST_MIN", "3"))           # cars/burst lower bound (forever mode)
BURST_MAX   = int(os.getenv("BURST_MAX", "5"))           # cars/burst upper bound (forever mode)

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


def _make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks=ACKS,
        compression_type=COMPRESSION,
        linger_ms=LINGER_MS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: (k.encode("utf-8") if isinstance(k, str) else k),
        retries=5,
        max_in_flight_requests_per_connection=5,  # plays nicely with acks='all'
        api_version_auto_timeout_ms=60000,
    )


def run_finite():
    """Send exactly NUM_RECORDS using the generator's finite stream."""
    producer = _make_producer()
    sent = 0
    try:
        for rec in generate_records(NUM_RECORDS):
            payload = _to_db_schema(rec)
            key = payload["pmgid"]  # stable partitioning by sensor
            producer.send(TOPIC, key=key, value=payload)
            sent += 1
        producer.flush()
        print(f"[producer] Sent {sent} records to '{TOPIC}' with RUN_ID={RUN_ID}")
    finally:
        try:
            producer.flush()
        except Exception:
            pass
        try:
            producer.close()
        except Exception:
            pass


def run_forever():
    """
    Stream bursts indefinitely.
    Pacing varies by daypart:
      - Rush (volume_mult ~1.5–2.0)  -> shorter pause (faster)
      - Late (volume_mult ~0.3–0.5)  -> longer pause (slower)
    """
    producer = _make_producer()
    sent = 0
    pmg_index = 0

    def next_pmgid():
        nonlocal pmg_index
        pmg_index += 1
        # keep IDs rolling: PMG00001, PMG00002, ..., PMG09999, PMG10000, PMG10001, ...
        return f"PMG{pmg_index:05d}"

    print(f"[producer] RUN_FOREVER=1  topic='{TOPIC}'  RUN_ID={RUN_ID}  BASE_SLEEP={BASE_SLEEP}s")
    try:
        while True:
            pmgid = next_pmgid()
            # choose a base burst size; the generator will still apply volume logic internally
            base_cars = max(BURST_MIN, min(BURST_MAX, BURST_MIN + (pmg_index % (BURST_MAX - BURST_MIN + 1))))

            now_utc = datetime.now(timezone.utc)
            burst = generate_burst_for_sensor(pmgid, now_utc, cars_in_burst=base_cars)

            for rec in burst:
                payload = _to_db_schema(rec)
                key = payload["pmgid"]
                producer.send(TOPIC, key=key, value=payload)
                sent += 1

            producer.flush()

            # pacing: vary sleep by daypart (lower at rush, higher late night)
            local_now = now_local_chicago()
            _, volume_mult = daypart_multipliers(local_now)
            pause = BASE_SLEEP / max(0.1, volume_mult)  # rush (2.0x) → half sleep; late (0.3x) → ~3.3x sleep
            time.sleep(pause)

    except KeyboardInterrupt:
        print("\n[producer] Stopped by user (Ctrl+C).")
    finally:
        try:
            producer.flush()
        except Exception:
            pass
        try:
            producer.close()
        except Exception:
            pass
        print(f"[producer] Total sent this session: {sent} records  RUN_ID={RUN_ID}")


def main():
    if RUN_FOREVER:
        run_forever()
    else:
        run_finite()


if __name__ == "__main__":
    main()
