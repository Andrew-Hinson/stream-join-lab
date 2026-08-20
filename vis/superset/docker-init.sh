#!/usr/bin/env bash
set -euo pipefail

superset db upgrade
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@localhost \
  --password admin || true
superset init

python <<'PY'
import time
from trino.dbapi import connect

def connect_trino():
    return connect(host="trino", port=8080, user="trino", catalog="iceberg", http_scheme="http")

for i in range(30):
    try:
        cur = connect_trino().cursor()
        cur.execute("SHOW SCHEMAS")
        cur.fetchall()
        print("trino ready")
        break
    except Exception as e:
        print(f"trino not ready ({i}): {e}")
        time.sleep(2)

for i in range(15):
    try:
        cur = connect_trino().cursor()
        cur.execute("SHOW TABLES FROM demo")
        tables = [r[0] for r in cur.fetchall()]
        print("demo tables:", tables)
        if "match_facts" in tables:
            break
    except Exception as e:
        print(f"demo.match_facts wait ({i}): {e}")
        time.sleep(2)
PY

superset import-dashboards --path /app/match-facts.zip --username admin

exec /usr/bin/run-server.sh
