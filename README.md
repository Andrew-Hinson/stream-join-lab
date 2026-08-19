## Marvel Rivals Ranked Games Simulator
This is a Kafka-Flink-Iceberg demo with the purpose to show in flight stream joins outputting to Iceberg with built in real time metrics visualizations.

### How to use
```docker compose up --build```

WIP

How it works
- Docker Compose initializes postgres
- Apache Kafka is stood up
- Kafka Connect (debezium) is pointed at postgres
- Flink jobmanager is stood up
- Flink taskmanager is stood up
- data_generator.py runs, seeding Postgres with 100 random players
- match_participants.py runs, simulating game match results and writing to postgres
- Kafka connect picks up cdc changes and publishes to corresponding Kafka topics
- Flink job joins streams to produce "match facts" Iceberg table

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
check results after job is finished:
```docker compose logs -f readback```

Flink UI: 
[flink-ui](http://localhost:8081)

## Kcat-cli
Install `yay -S kcat-cli`

Once stack is up, leave running: `kcat -C -b localhost:9092 -t dbserver1.public.match_events -f '%s\n'`

In separate terminal run: `./scripts/run_matches.sh --count 10`

View metadata about broker: `kcat -L -b localhost:9092`

