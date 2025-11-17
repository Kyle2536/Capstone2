# consumer_kafka.py — LIVE profile (small, frequent flushes) but env-tunable
import os, json, time, signal, sys
from datetime import datetime, timezone
from typing import List, Dict

# --- robust shim for environments where kafka.vendor.six is missing ---
try:
    import six
    sys.modules['kafka.vendor.six'] = six
    sys.modules['kafka.vendor.six.moves'] = six.moves
except Exception:
    pass
# ----------------------------------------------------------------------

from dotenv import load_dotenv
load_dotenv()  # make .env visible

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from db_writer import write_batch, BUSINESS_TABLE, LAT_TABLE  # our DB writer


# =========================
# Config (env overrides)
# =========================
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
TOPIC     = os.getenv("KAFKA_TOPIC", "traffic.raw.v2")
GROUP     = os.getenv("KAFKA_GROUP", "kafka-rush-consumer")

# LIVE-ish defaults; override via env if you want larger batches
MAX_POLL_RECORDS = int(os.getenv("MAX_POLL_RECORDS", "200"))   # smaller fetches → smoother UI
FETCH_MIN_BYTES  = int(os.getenv("FETCH_MIN_BYTES", "1"))      # return asap, even for tiny payloads
FETCH_MAX_WAIT_MS= int(os.getenv("FETCH_MAX_WAIT_MS", "50"))   # don't linger waiting to coalesce
POLL_TIMEOUT_MS  = int(os.getenv("POLL_TIMEOUT_MS", "150"))    # main loop poll timeout

BATCH_ROWS = int(os.getenv("BATCH_ROWS", "200"))               # flush to DB when buffer hits this
FLUSH_MS   = int(os.getenv("FLUSH_MS", "300"))                 # or every N ms (monotonic), whichever first

# Optional: exit if nothing arrives for a while (keeps CLI runs from hanging forever)
AUTO_EXIT_IDLE_S = int(os.getenv("AUTO_EXIT_IDLE_S", "0"))     # 0 = never auto-exit

# Logging cadence
LOG_EVERY = int(os.getenv("LOG_EVERY", "5000"))                # log per N rows written

# Offset reset default: “latest” for tailing new data in demos (env can override)
AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")


_running = True
def _graceful(_sig, _frm):
    global _running
    _running = False
signal.signal(signal.SIGINT, _graceful)
signal.signal(signal.SIGTERM, _graceful)


def utc_now_iso_ms() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def parse_message(raw) -> Dict:
    """
    Value is JSON with keys produced by producer_kafka/generator:
      run_id, trace_id, created_at, ts, peakspeed, pmgid, direction, location, vehiclecount, sensor_created_at
    """
    d = json.loads(raw)
    # tiny guard: ensure required fields exist
    required = ["run_id", "trace_id", "created_at", "ts", "peakspeed",
                "pmgid", "direction", "location", "vehiclecount", "sensor_created_at"]
    for k in required:
        if k not in d:
            raise ValueError(f"missing field {k} in message")
    return d


def flush_batch(consumer: KafkaConsumer, buffer: List[Dict], run_id: str, total_written: int):
    """Write buffer to DB and commit offsets only if DB write succeeds."""
    if not buffer:
        return total_written

    ingest_ts_utc = utc_now_iso_ms()

    # DB write (business + latency; single transaction inside write_batch)
    start_id, n = write_batch(run_id, ingest_ts_utc, buffer)

    total_written += n
    if (total_written % LOG_EVERY == 0) or (n >= BATCH_ROWS):
        print(f"[consumer] wrote {n} rows (record_id start≈{start_id}) "
              f"→ totals={total_written} run={run_id}", flush=True)

    # Only commit if DB write succeeded
    consumer.commit()
    return total_written


def main():
    print(f"[consumer] Listening on topic '{TOPIC}' → business: {BUSINESS_TABLE}, lat: {LAT_TABLE}")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP,
        enable_auto_commit=False,               # we commit after DB success
        auto_offset_reset=AUTO_OFFSET_RESET,    # 'latest' by default for tailing demos
        max_poll_records=MAX_POLL_RECORDS,
        fetch_min_bytes=FETCH_MIN_BYTES,
        fetch_max_wait_ms=FETCH_MAX_WAIT_MS,
        consumer_timeout_ms=0,                  # never auto-timeout the iterator
        value_deserializer=lambda v: v,         # parse JSON manually
    )

    buffer: List[Dict] = []
    run_id_for_batch = None
    last_flush_ms = time.monotonic() * 1000.0   # monotonic for robustness
    total_written = 0
    last_activity = time.monotonic()

    try:
        while _running:
            # Poll for up to POLL_TIMEOUT_MS
            msgs = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=MAX_POLL_RECORDS)
            got = 0

            # Flatten the polled batches
            for _tp, records in msgs.items():
                for rec in records:
                    try:
                        d = parse_message(rec.value)
                        d["peakspeed"] = int(round(float(d["peakspeed"])))  # no decimals
                    except Exception as e:
                        print(f"[consumer] skip bad message: {e}", flush=True)
                        continue

                    if run_id_for_batch is None:
                        run_id_for_batch = d["run_id"]

                    buffer.append(d)
                    got += 1

            now_ms = time.monotonic() * 1000.0
            if got > 0:
                last_activity = time.monotonic()

            # Flush if big enough or time window reached
            if buffer and (len(buffer) >= BATCH_ROWS or (now_ms - last_flush_ms) >= FLUSH_MS):
                try:
                    total_written = flush_batch(consumer, buffer, run_id_for_batch, total_written)
                    buffer.clear()
                    run_id_for_batch = None
                    last_flush_ms = now_ms
                except Exception as e:
                    # Do not commit; surface the error and keep running
                    print(f"[consumer] DB write failed: {e}", flush=True)
                    time.sleep(0.25)  # tiny backoff
                    # Optionally: dead-letter to a file here

            # Optional auto-exit when idle and buffer empty
            if AUTO_EXIT_IDLE_S and not buffer and (time.monotonic() - last_activity) > AUTO_EXIT_IDLE_S:
                print(f"[consumer] idle {AUTO_EXIT_IDLE_S}s → exiting cleanly.", flush=True)
                break

        # Final flush on shutdown
        if buffer:
            try:
                total_written = flush_batch(consumer, buffer, run_id_for_batch, total_written)
                print(f"[consumer] final flush {len(buffer)} rows", flush=True)
            except Exception as e:
                print(f"[consumer] final flush failed: {e}", flush=True)

    except KafkaError as ke:
        print(f"[consumer] Kafka error: {ke}", flush=True)
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        print(f"[consumer] stopped. total_written={total_written}", flush=True)


if __name__ == "__main__":
    main()
