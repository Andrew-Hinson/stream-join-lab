#!/bin/sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
POSTGRES_USER="${POSTGRES_USER:-demo}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-demo}"
POSTGRES_DB="${POSTGRES_DB:-stream_join_lab}"

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
