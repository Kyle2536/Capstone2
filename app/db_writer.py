# db_writer.py
# Writes business rows + latency stamps for the rush-hour Kafka pipeline.
# Robustly maps record_id via trace_id (no reliance on AUTO_INCREMENT math).

import os
from typing import List, Dict, Tuple
import pymysql
from pymysql.cursors import DictCursor
from dotenv import load_dotenv

load_dotenv()

# =========================
# DB configuration (env)
# =========================
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "password")
DB_NAME = os.getenv("DB_NAME", "capstone")

DB_SSL = os.getenv("DB_SSL", "false").lower() == "true"
DB_SSL_CA = os.getenv("DB_SSL_CA")  # optional path to CA pem

BUSINESS_TABLE = os.getenv("BUSINESS_TABLE", "kafka_pipeline_rush")
LAT_TABLE = os.getenv("LAT_TABLE", "kafka_latencies_rush")

# =========================
# DDL (tables + indexes)
# =========================
CREATE_BUSINESS_SQL = f"""
CREATE TABLE IF NOT EXISTS {BUSINESS_TABLE} (
  record_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  run_id VARCHAR(32) NOT NULL,
  trace_id CHAR(36) NOT NULL,                -- NEW: trace_id saved in business
  created_at DATE NOT NULL,
  ts TIME(3) NOT NULL,
  peakspeed DOUBLE NOT NULL,
  pmgid VARCHAR(32) NOT NULL,
  direction INT NOT NULL,
  location VARCHAR(64) NOT NULL,
  vehiclecount INT NOT NULL,
  UNIQUE KEY uq_{BUSINESS_TABLE}_trace (trace_id)  -- fast lookup for mapping
) ENGINE=InnoDB;
"""

CREATE_LATENCY_SQL = f"""
CREATE TABLE IF NOT EXISTS {LAT_TABLE} (
  trace_id CHAR(36) PRIMARY KEY,
  run_id VARCHAR(32) NOT NULL,
  record_id BIGINT NOT NULL,
  sensor_created_at DATETIME(3) NOT NULL,
  ingest_received_at DATETIME(3) NOT NULL,
  sql_written_at DATETIME(3) NOT NULL,
  dashboard_emitted_at DATETIME(3) DEFAULT NULL,
  dashboard_rendered_at DATETIME(3) DEFAULT NULL,
  CONSTRAINT fk_rush_record
    FOREIGN KEY (record_id) REFERENCES {BUSINESS_TABLE}(record_id)
    ON DELETE CASCADE
) ENGINE=InnoDB;
"""

CREATE_INDEXES = [
    f"CREATE INDEX IF NOT EXISTS idx_{BUSINESS_TABLE}_pmgid ON {BUSINESS_TABLE}(pmgid);",
    f"CREATE INDEX IF NOT EXISTS idx_{BUSINESS_TABLE}_time  ON {BUSINESS_TABLE}(created_at, ts);",
    f"CREATE INDEX IF NOT EXISTS idx_{LAT_TABLE}_run       ON {LAT_TABLE}(run_id);",
    f"CREATE INDEX IF NOT EXISTS idx_{LAT_TABLE}_record    ON {LAT_TABLE}(record_id);",
]

# =========================
# SQL inserts / selects
# =========================
INSERT_BUSINESS = f"""
INSERT INTO {BUSINESS_TABLE}
(run_id, trace_id, created_at, ts, peakspeed, pmgid, direction, location, vehiclecount)
VALUES (%(run_id)s, %(trace_id)s, %(created_at)s, %(ts)s, %(peakspeed)s, %(pmgid)s, %(direction)s, %(location)s, %(vehiclecount)s)
"""

INSERT_LATENCY = f"""
INSERT INTO {LAT_TABLE}
(trace_id, run_id, record_id, sensor_created_at, ingest_received_at, sql_written_at)
VALUES (%s, %s, %s, %s, %s, NOW(3))
"""

SELECT_RECORD_IDS_BY_TRACE = f"""
SELECT trace_id, record_id
FROM {BUSINESS_TABLE}
WHERE trace_id IN ({'{placeholders}'})
"""

# =========================
# Connection helper
# =========================
def get_conn():
    kwargs = dict(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        autocommit=False,
        cursorclass=DictCursor,
        charset="utf8mb4",
    )
    if DB_SSL:
        ssl_kwargs = {}
        if DB_SSL_CA:
            ssl_kwargs["ca"] = DB_SSL_CA
        kwargs["ssl"] = ssl_kwargs or {"ssl": {}}
    return pymysql.connect(**kwargs)

# =========================
# Ensure schema (idempotent)
# =========================
def ensure_tables():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_BUSINESS_SQL)
            cur.execute(CREATE_LATENCY_SQL)
            for stmt in CREATE_INDEXES:
                try:
                    cur.execute(stmt)
                except Exception:
                    pass
        conn.commit()

# If you previously created {BUSINESS_TABLE} without trace_id, run once:
#   ALTER TABLE kafka_pipeline_rush ADD COLUMN trace_id CHAR(36) NOT NULL UNIQUE AFTER run_id;

# =========================
# Write helpers
# =========================
def insert_business_batch(conn, business_rows: List[Dict]) -> None:
    """Insert business rows (trace_id included)."""
    if not business_rows:
        return
    with conn.cursor() as cur:
        cur.executemany(INSERT_BUSINESS, business_rows)

def fetch_record_ids_by_trace(conn, trace_ids: List[str]) -> Dict[str, int]:
    """Return {trace_id: record_id} for the given trace_ids."""
    if not trace_ids:
        return {}
    placeholders = ",".join(["%s"] * len(trace_ids))
    sql = SELECT_RECORD_IDS_BY_TRACE.format(placeholders=placeholders)
    with conn.cursor() as cur:
        cur.execute(sql, trace_ids)
        rows = cur.fetchall()
    return {r["trace_id"]: r["record_id"] for r in rows}

def insert_latency_batch(conn, run_id: str, ingest_ts_utc: str, rows: List[Dict], trace_to_id: Dict[str, int]):
    """Insert latency rows using resolved record_ids from business table."""
    vals = []
    for row in rows:
        rec_id = trace_to_id.get(row["trace_id"])
        if rec_id is None:
            # If a mapping is missing, skip to avoid FK error (or raise)
            raise RuntimeError(f"Missing record_id for trace_id {row['trace_id']}")
        vals.append((
            row["trace_id"],
            run_id,
            rec_id,
            row["sensor_created_at"],
            ingest_ts_utc,
        ))
    if not vals:
        return
    with conn.cursor() as cur:
        cur.executemany(INSERT_LATENCY, vals)

def write_batch(run_id: str, ingest_ts_utc, rows: List[Dict]) -> Tuple[int, int]:
    """
    Insert business + latency in one transaction using trace_id mapping.
    Returns (min_record_id_in_batch, count).
    """
    if not rows:
        return (0, 0)
    ensure_tables()
    with get_conn() as conn:
        # 1) Business rows (with trace_id)
        insert_business_batch(conn, rows)
        # 2) Resolve assigned record_ids by trace_id
        trace_ids = [r["trace_id"] for r in rows]
        trace_to_id = fetch_record_ids_by_trace(conn, trace_ids)
        # 3) Latency rows with exact record_ids
        insert_latency_batch(conn, run_id, ingest_ts_utc, rows, trace_to_id)
        conn.commit()

        # For logging
        rec_ids = list(trace_to_id.values())
        return (min(rec_ids) if rec_ids else 0, len(rec_ids))
