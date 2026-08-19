## Marvel Rivals Ranked Games Simulator

This is a Kafka-Flink-Iceberg demo with the purpose to show in flight stream joins outputting to Iceberg with built in real time metrics visualizations.

### How it works

- Compose brings up Postgres, Kafka, Debezium, Flink, Iceberg REST, and Silo (S3)
- Seed inserts 48 players into Postgres
- matches simulates ranked games (12 players each) into Postgres: events, participants, rank updates
- Debezium CDC publishes those writes to Kafka
- Flink joins the streams into match facts (player + match + rank as-of match start)
- Facts append to Iceberg table demo.match_facts (Parquet on Silo)
- readback scans that table over REST + S3 and checks row count and rank-as-of

### How to use

`docker compose up --build`

### Common Commands

Confirm connector is running: 
`curl -s http://localhost:8083/connectors/postgres-cdc/status | jq .`

List CDC Topics
`docker compose exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`

Watch Topic

```docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dbserver1.public.players \
  --from-beginning
```

Connect to running Postgres

```
set -a && source .env && set +a
docker compose exec db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

Issue change in Postgres

```
set -a && source .env && set +a
docker compose exec db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "INSERT INTO players (account_name, email) VALUES ('cdc-test', 'cdc-test@example.com');"
```

check results after matches exits:
`docker compose logs readback`

Flink UI: 
[flink-ui](http://localhost:8081)

## Kcat-cli

Install `yay -S kcat-cli`

Once stack is up, leave running: `kcat -C -b localhost:9092 -t dbserver1.public.match_events -f '%s\n'`

In separate terminal run: `./scripts/run_matches.sh --count 10`

View metadata about broker: `kcat -L -b localhost:9092`