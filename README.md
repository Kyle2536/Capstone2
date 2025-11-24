Capstone2 — Runbook (Kafka → SQL → Dashboard)

0) Prereqs
- Docker Desktop running
- Python 3.11 (recommended)
- PowerShell on Windows
- MySQL/Azure DB credentials with allow-listed IP (if Azure)

1) Project layout
C:\Capstone2\
│  .env
│  docker-compose.yml
│  requirements.txt
│
├─ app\
│   ├─ producer_kafka.py
│   ├─ consumer_kafka.py
│   ├─ generator_fixed.py
│   ├─ db_writer.py
│   ├─ web_server.py
│   └─ static\
│       └─ dashboard.html
└─ sql\
    └─ capstone_latency_suite.sql

2) Create venv & install deps
PS> cd C:\Capstone2
PS> py -3.11 -m venv .venv
PS> .\.venv\Scripts\Activate.ps1
PS> python -m pip install -U pip setuptools wheel
PS> pip install --no-cache-dir -r requirements.txt

requirements.txt should include:
flask
pymysql
python-dotenv
kafka-python==2.0.2
cryptography
lz4
six
requests

3) Configure .env  (C:\Capstone2\.env)
# --- Kafka ---
KAFKA_BOOTSTRAP=127.0.0.1:9092
KAFKA_TOPIC=traffic.raw.v2
KAFKA_GROUP=kafka-rush-consumer

# Producer tuning
NUM_RECORDS=100
KAFKA_LINGER_MS=0
KAFKA_ACKS=1

# Consumer tuning (small, frequent flushes)
MAX_POLL_RECORDS=500
FETCH_MIN_BYTES=1
FETCH_MAX_WAIT_MS=25
POLL_TIMEOUT_MS=75
BATCH_ROWS=100
FLUSH_MS=100
KAFKA_AUTO_OFFSET_RESET=latest

# --- MySQL / Azure ---
DB_HOST=richardsonsql.mysql.database.azure.com
DB_PORT=3306
DB_USER=utdsql
DB_PASS=YOUR_PASSWORD_HERE
DB_NAME=kafka
DB_SSL=true

4) Start Kafka (Redpanda)
PS> cd C:\Capstone2
PS> docker compose up -d
# (optional check)
PS> docker ps
PS> docker exec -it redpanda rpk topic list
# If needed:
# PS> docker exec -it redpanda rpk topic create traffic.raw.v2

5) Start the consumer (Kafka → MySQL)
PS> cd C:\Capstone2
PS> .\.venv\Scripts\Activate.ps1
PS> python .\app\consumer_kafka.py
Expected log:
[consumer] Listening on topic 'traffic.raw.v2' → business: kafka_pipeline_rush, lat: kafka_latencies_rush
[consumer cfg] MAX_POLL_RECORDS=... | FETCH_MAX_WAIT_MS=... | BATCH_ROWS=... | FLUSH_MS=... | ...

6) Start the dashboard server
# New PowerShell window
PS> cd C:\Capstone2
PS> .\.venv\Scripts\Activate.ps1
PS> python .\app\web_server.py
Open: http://127.0.0.1:5000/dashboard

7) Run the producer (sensor → Kafka)
# New PowerShell window
PS> cd C:\Capstone2
PS> .\.venv\Scripts\Activate.ps1
# (optional per-run overrides)
PS> $env:KAFKA_BOOTSTRAP="127.0.0.1:9092"
PS> $env:KAFKA_TOPIC="traffic.raw.v2"
PS> $env:NUM_RECORDS="100"
PS> python .\app\producer_kafka.py

Expected log:
[producer] Sent 100 records to topic 'traffic.raw.v2' with RUN_ID=run_YYYYmmddTHHMMSS

8) (Optional) Latency analysis SQL
Run sql\capstone_latency_suite.sql in your MySQL client to analyze latest run_id.

Quick Troubleshooting
- NoBrokersAvailable: Start Redpanda; check KAFKA_BOOTSTRAP=127.0.0.1:9092
- MySQL 3159 (secure transport): Set DB_SSL=true; ensure Azure allows your IP
- Foreign key error inserting latencies: Use the updated db_writer.py (business then latencies in one transaction)
- Dashboard not refreshing: Keep @after_request no-cache headers; POLL_MS in dashboard.html ~100–350ms
- Producer still sends 1000: Confirm NUM_RECORDS in .env or set $env:NUM_RECORDS per run

Key Objects
- Kafka topic: traffic.raw.v2
- Business table: kafka_pipeline_rush
- Latency table: kafka_latencies_rush
- Dashboard URL: http://127.0.0.1:5000/dashboard

Files
- app/producer_kafka.py
- app/consumer_kafka.py
- app/generator_fixed.py
- app/db_writer.py
- app/web_server.py
- app/static/dashboard.html
- docker-compose.yml
- sql/capstone_latency_suite.sql
