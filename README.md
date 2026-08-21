## Marvel Rivals Ranked Games Simulator

A Kafka → Debezium → Flink → Iceberg CDC pipeline. Simulates ranked Marvel Rivals matches, streams the writes out of Postgres via CDC, joins them in-flight with Flink, and lands enriched match facts in Iceberg.

## Run

MacOS, Linux, or Windows. A 16GB laptop is the intended box; set the Docker memory slider to at least 8 GiB. `./up` fails below 6 GiB.Needs Docker Desktop (or Engine + Compose v2) on m

**Every `./up` is a Clean start:** it destroys Lab volumes, then builds and starts. First run is about 20 minutes (image pulls + Maven). Later runs are faster but still wipe data and replay matches. Use the URLs it prints. Ports remap if 8088/3000/8081 are taken.

Unzip or clone, `cd` into this folder, then:

**macOS / Linux / WSL**

```bash
./up
```

**Windows**

```bat
.\up.cmd
```

`./up` waits until `match_facts` has at least one row (Ready), then opens [Match facts](http://127.0.0.1:8088/superset/dashboard/match-facts/) and [Grafana Pipeline health](http://127.0.0.1:3000/d/pipeline-health). Charts keep filling (default 400 matches, about 4800 facts). Stop with `./down` (or `.\down.cmd`).

## Architecture

```mermaid
flowchart LR
    subgraph Source
        PG[(Postgres)]
    end
    subgraph CDC
        DBZ[Debezium]
    end
    subgraph Streaming
        KFK[[Kafka]]
        FLK[Flink Job]
    end
    subgraph Lakehouse
        ICE[(Iceberg REST Catalog)]
        S3[Silo / S3 storage]
    end
    subgraph Vis
        TR[Trino]
        SS[Superset]
    end

    PG -- WAL --> DBZ
    DBZ -- CDC events --> KFK
    KFK -- players / matches / participants / rank_updates --> FLK
    FLK -- match_facts --> ICE
    ICE --- S3
    ICE --> TR
    TR --> SS
```

## Stack

| Component | Role |
|---|---|
| Postgres | System of record: players, matches, participants, rank updates |
| Debezium | CDC connector, publishes row-level changes to Kafka |
| Kafka | Event backbone for CDC topics |
| Flink | Stateful stream processing: joins CDC streams into enriched match facts |
| Iceberg (REST catalog) | Table format for streaming writes |
| Silo (S3) | Object storage backing the Iceberg warehouse |
| Trino | SQL engine over Iceberg |
| Superset | Match facts vis: dashboards and SQL Lab |

What `./up` starts:

1. **Seed** inserts 48 players into Postgres
2. **matches** simulates ranked games (12 players each) into Postgres: events, participants, rank updates
3. **Debezium** CDC publishes those writes to Kafka
4. **Flink** joins the streams into match facts (player + match + rank as-of match start)
5. Facts append to the Iceberg table `demo.match_facts` (Parquet on Silo)
6. **Superset** queries that table through Trino

## Query match facts

SQL Lab is on the Superset URL `./up` printed (`/sqllab`). Database: **Iceberg** (read-only). Catalog `iceberg`, schema `demo`.

```sql
SELECT COUNT(*) AS facts
FROM iceberg.demo.match_facts;
```

```sql
SELECT
  hero_played,
  ROUND(100e0 * AVG(IF(result = 'win', 1e0, 0e0)), 1) AS win_rate,
  ROUND(AVG(CAST(kills AS DOUBLE)), 2) AS avg_kills,
  ROUND(AVG(CAST(deaths AS DOUBLE)), 2) AS avg_deaths
FROM iceberg.demo.match_facts
GROUP BY 1
ORDER BY win_rate DESC;
```

```sql
SELECT
  player_id,
  started_at,
  result,
  rank_tier_at_match_start,
  rank_division_at_match_start,
  rank_points_at_match_start
FROM iceberg.demo.match_facts
ORDER BY player_id, started_at
LIMIT 20;
```

## Pipeline metrics

Grafana is the URL `./up` printed (`/d/pipeline-health`). Same instance has **Flink** and **Kafka** dashboards. Flink UI URL is printed, not opened. See [Pipeline alerting](docs/alerting.md) for page versus ticket rules.

## Inspect

Use the printed URLs. Extra traffic: `docker compose --env-file .lab.env run --rm matches`. Kafka, Connect, Trino, and Prometheus are not published on the host.

**CDC connector**

```bash
docker compose --env-file .lab.env exec connect \
  curl -s http://localhost:8083/connectors/postgres-cdc/status
```

**List CDC topics**

```bash
docker compose --env-file .lab.env exec broker \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Watch a topic**

```bash
docker compose --env-file .lab.env exec broker \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dbserver1.public.players \
  --from-beginning
```

**Postgres**

```bash
docker compose --env-file .lab.env exec db psql -U demo -d stream_join_lab
```

**Manual change (watch CDC propagate)**

```bash
docker compose --env-file .lab.env exec db psql -U demo -d stream_join_lab -c \
  "INSERT INTO players (account_name, email) VALUES ('cdc-test', 'cdc-test@example.com');"
```
