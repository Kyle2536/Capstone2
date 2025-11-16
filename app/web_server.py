import os
import json
import pymysql
from flask import Flask, jsonify, request, send_from_directory, make_response
from datetime import timezone
from dotenv import load_dotenv

# ---------------------------------------------------------------------
# 🌍 Load environment variables
# ---------------------------------------------------------------------
load_dotenv()

# Accept both DB_PASS and DB_PASSWORD for convenience
_db_pass = os.getenv("DB_PASS", os.getenv("DB_PASSWORD"))

DB = dict(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=_db_pass,
    db=os.getenv("DB_NAME", "capstone"),
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

if os.getenv("DB_SSL", "false").lower() == "true":
    DB["ssl"] = {"ssl": {}}

# ---------------------------------------------------------------------
# 📦 Target tables (rush-hour pipeline)
# ---------------------------------------------------------------------
BUSINESS_TABLE = "kafka_pipeline_rush"
LATENCY_TABLE = "kafka_latencies_rush"

# ---------------------------------------------------------------------
# ⚙️ Flask setup
# ---------------------------------------------------------------------
app = Flask(__name__, static_folder="static", static_url_path="/static")

def get_conn():
    """Get a new pymysql connection."""
    return pymysql.connect(**DB)

# ---------------------------------------------------------------------
# 📝 tiny logger (prints to Flask console)
# ---------------------------------------------------------------------
def log(msg):
    try:
        print(msg, flush=True)
    except Exception:
        pass

# optional but helpful for static/template freshness in dev
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.config['TEMPLATES_AUTO_RELOAD'] = True

@app.after_request
def add_no_cache(resp):
    # Strong anti-cache headers for every response
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    # ETag can also cause 304s — disable if you still see caching
    resp.headers.pop('ETag', None)
    return resp

# ---------------------------------------------------------------------
# 🧠 API: /api/latest
# Returns recent records > sinceId and stamps dashboard_emitted_at
# ---------------------------------------------------------------------
# ---------------------------------------------------------------------
# 🧠 API: /api/latest
# Returns recent records > sinceId and stamps dashboard_emitted_at
# ---------------------------------------------------------------------
@app.route("/api/latest")
def api_latest():
    since_id = int(request.args.get("sinceId", 0))
    limit = int(request.args.get("limit", 500))
    rows = []
    max_id = since_id

    # Include direction (forced to integer) and any other columns you want on the client.
    sql = f"""
      SELECT
        l.record_id,
        l.trace_id,
        l.run_id,
        l.sensor_created_at,
        p.pmgid,
        p.peakspeed,
        (p.direction + 0) AS direction,   -- ensure 0/1 numeric
        p.vehiclecount,
        p.location
      FROM {LATENCY_TABLE} l
      JOIN {BUSINESS_TABLE} p
        ON p.record_id = l.record_id
      WHERE l.record_id > %s
      ORDER BY l.record_id ASC
      LIMIT %s
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Force UTC from server to keep timestamps consistent
            cur.execute("SET time_zone = '+00:00'")
            cur.execute(sql, (since_id, limit))
            rows = cur.fetchall()

            if rows:
                trace_ids = [r["trace_id"] for r in rows]
                placeholders = ",".join(["%s"] * len(trace_ids))
                upd = f"""
                    UPDATE {LATENCY_TABLE}
                    SET dashboard_emitted_at = NOW(3)
                    WHERE trace_id IN ({placeholders})
                      AND dashboard_emitted_at IS NULL
                """
                cur.execute(upd, trace_ids)
                conn.commit()
                max_id = max(r["record_id"] for r in rows)

    # Convert timestamps to ISO UTC for JS
    for r in rows:
        if r.get("sensor_created_at"):
            r["sensor_created_at"] = (
                r["sensor_created_at"]
                .replace(tzinfo=timezone.utc)
                .isoformat(timespec="milliseconds")
            )
        # Defensive: if any adapter still returns direction as None/str, coerce to 0/1
        d = r.get("direction", 0)
        try:
            r["direction"] = 1 if int(d) == 1 else 0
        except Exception:
            r["direction"] = 0

    log(f"[latest] sinceId={since_id} -> returned={len(rows)} maxId={max_id}")
    return jsonify({"rows": rows, "maxId": max_id})


# ---------------------------------------------------------------------
# 🧠 API: /latency/ack-bulk
# Stamps dashboard_rendered_at for acknowledged trace_ids (bulk)
# ---------------------------------------------------------------------
@app.route("/latency/ack-bulk", methods=["POST"])
def latency_ack_bulk():
    data = request.get_json(silent=True) or {}
    acks = data.get("acks", [])
    trace_ids = [a.get("trace_id") for a in acks if a.get("trace_id")]
    if not trace_ids:
        log("[ack-bulk] recv=0 (no trace_ids)")
        return jsonify({"count": 0, "acked": []})

    placeholders = ",".join(["%s"] * len(trace_ids))

    update_sql = f"""
        UPDATE {LATENCY_TABLE}
        SET dashboard_rendered_at = NOW(3)
        WHERE trace_id IN ({placeholders})
          AND dashboard_rendered_at IS NULL
    """
    read_sql = f"""
        SELECT trace_id
        FROM {LATENCY_TABLE}
        WHERE trace_id IN ({placeholders})
          AND dashboard_rendered_at IS NOT NULL
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET time_zone = '+00:00'")
            log(f"[ack-bulk] recv={len(trace_ids)}")
            cur.execute(update_sql, trace_ids)
            affected = cur.rowcount
            conn.commit()

            cur.execute(read_sql, trace_ids)
            done = [r["trace_id"] for r in cur.fetchall()]

    log(f"[ack-bulk] updated={affected}, confirmed={len(done)}")
    return jsonify({"count": affected, "acked": done})

# ---------------------------------------------------------------------
# 🧠 API: /latency/ack
# Backward-compatible single ACK route
# ---------------------------------------------------------------------
@app.route("/latency/ack", methods=["POST"])
def latency_ack_single():
    data = request.get_json(silent=True) or {}
    trace_id = data.get("trace_id")
    if not trace_id:
        return jsonify({"count": 0}), 400

    sql = f"""
      UPDATE {LATENCY_TABLE}
      SET dashboard_rendered_at = NOW(3)
      WHERE trace_id = %s
        AND dashboard_rendered_at IS NULL
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET time_zone = '+00:00'")
            cur.execute(sql, (trace_id,))
            affected = cur.rowcount
            conn.commit()

    log(f"[ack] {trace_id} -> {affected}")
    return jsonify({"count": affected})

# ---------------------------------------------------------------------
# 🧠 API: /api/head
# Return current max(record_id) so the UI can start at the head (tail mode).
# ---------------------------------------------------------------------
@app.route("/api/head")
def api_head():
    head = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET time_zone = '+00:00'")
            cur.execute(f"SELECT COALESCE(MAX(record_id), 0) AS head FROM {LATENCY_TABLE}")
            row = cur.fetchone()
            head = row["head"] or 0
    try:
        print(f"[head] {head}", flush=True)
    except Exception:
        pass
    return jsonify({"head": head})

# ---------------------------------------------------------------------
# 📊 Dashboard route
# ---------------------------------------------------------------------
@app.route("/dashboard")
def dashboard():
    return send_from_directory("static", "dashboard.html")

# ---------------------------------------------------------------------
# 🏁 Entry point
# ---------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    print(f"🚀 Flask dashboard running on http://127.0.0.1:{port}/dashboard")
    app.run(host="0.0.0.0", port=port, debug=True)
