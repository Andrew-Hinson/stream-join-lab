#!/bin/bash/env bash

set -euo pipefail

set -a
source "$(dirname "$0")/../.env"
set +a

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

echo "Waiting for Connect at $CONNECT_URL ..."
until curl -sf "$CONNECT_URL" > /dev/null; do
    sleep 2
done

curl -sf -X PUT "$CONNECT_URL/connectors/postgres-cdc/config" \
    -H "Content-Type: application/json" \
    -d "{
    \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
    \"tasks.max\": \"1\",
    \"database.hostname\": \"db\",
    \"database.port\": \"5432\",
    \"database.user\": \"${POSTGRES_USER}\",
    \"database.password\": \"${POSTGRES_PASSWORD}\",
    \"database.dbname\": \"${POSTGRES_DB}\",
    \"topic.prefix\": \"dbserver1\",
    \"plugin.name\": \"pgoutput\",
    \"publication.name\": \"cdc_pub\",
    \"slot.name\": \"debezium_slot\",
    \"table.include.list\": \"public.players,public.ranks,public.match_events,public.match_participants\"
  }"

echo
echo "Registered. Status:"
curl -s "$CONNECT_URL/connectors/postgres-cdc/status"
echo