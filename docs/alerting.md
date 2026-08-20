# Pipeline alerting

Prometheus evaluates the rules in `observability/prometheus/alerts.yml`. There is no Alertmanager or pager integration. Firing rules show up on the Grafana **Pipeline health** alert list.

Severity is a Prometheus `severity` label: `page` (urgent and actionable) or `ticket` (investigate, not wake anyone). Dashboards also plot series that have no rule.

## What each dashboard is for

- **Pipeline health** is the hop narrative: Postgres, Kafka, Connect, Flink, Silo, then lag and join quality along the path.
- **Flink** is job golden signals: uptime, restarts, records in and out, checkpoint duration, backpressure, operator time, heap, and GC.
- **Kafka** is broker and consumer drill-down: replica gauges, `flink-stream-join` lag, messages and bytes, Produce and Fetch `TotalTimeMs`, request queue, and controller internals.

Open [Pipeline health](http://localhost:3000/d/pipeline-health), [Flink](http://localhost:3000/d/flink/flink), or [Kafka](http://localhost:3000/d/kafka/kafka).

## Severity table

| Kind | Alert | PromQL | Why |
|---|---|---|---|
| Page | Offline partitions | `kafka_controller_kafkacontroller_value{name="OfflinePartitionsCount"} > 0` | Partitions with no leader. Facts cannot land. Inert on this lab (see [Replication factor 1](#replication-factor-1)). |
| Page | Consumer lag climbing | `flink-stream-join` lag `> 200` **and** `deriv(...)[10m:] > 5` | Backlog is high and still growing. A stopped Flink job can demo this. |
| Page | Failed checkpoints | `increase(flink_jobmanager_job_numberOfFailedCheckpoints[15m]) >= 3` | The job cannot persist state. |
| Page | Job restart loop | `increase(flink_jobmanager_job_fullRestarts[10m]) >= 2` | The job is not staying up. |
| Page | CDC source stopped | `pg_debezium_slot_active == 0` for 1m | Change data capture (CDC) is not consuming the write-ahead log (WAL). |
| Ticket | Under-replicated partitions | `kafka_server_replicamanager_value{name="UnderReplicatedPartitions"} > 0` | Replica health, not a page on RF=1. |
| Ticket | Checkpoint duration high | `flink_jobmanager_job_lastCheckpointDuration > 10000` for 5m | Slow checkpoints. The 10s interval is the SLO-shaped line on **Flink**, not a page. |
| Ticket | Operator backpressure | watched `backPressuredTimeMsPerSecond > 500` for 15m | A cause, not a user-visible outage by itself. |
| Ticket | TaskManager heap high | heap used / max `> 0.8` for 10m | Cause. Watch **Flink** JVM heap. |
| Ticket | Streaming source lag high | `MilliSecondsBehindSource > 5000` for 5m | Debezium is behind Postgres. Not the same as Kafka consumer lag. |
| Ticket | Incomplete join rate | `rate(flink_taskmanager_job_task_operator_join_incomplete[5m]) > 0` for 5m | Join quality. Incomplete matches still write; this is not a page. |
| Dashboard only | Hop waterfall, Kafka internals, operator busy/idle, Produce/Fetch latency | no rule | Context for the tickets and pages. Java Management Extensions (JMX) series such as request queue and ISR shrinks have no alert. |

## Lag: floor plus derivative

A flat backlog means the consumer is keeping up at the current offset. Paging on lag alone trains you to ignore the rule.

The page requires both:

- Floor: `sum by (topic) (kafka_consumergroup_lag{consumergroup="flink-stream-join",topic=~"dbserver1\\.public\\..+"}) > 200`
- Slope: `deriv` of that sum over `[10m:]` `> 5` (messages per second)

Lab floors (200 messages, 5/s) are small enough that `docker compose stop` of the Flink taskmanager can pending or fire the rule. Production-scale numbers for the same idea are on the order of 100k lag and 500 messages per second. Do not copy the lab floors into a real cluster.

The Kafka dashboard lag derivative panel uses a 5m window. The page rule uses 10m so a brief hitch does not page.

## Replication factor 1

This compose stack runs one broker with replication factor 1 (`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` in `compose.yaml`).

These stay 0 unless the broker is down:

- `OfflinePartitionsCount` (page template)
- `UnderReplicatedPartitions` (ticket template)
- ISR shrinks on the **Kafka** dashboard

## Demo a page

With the pipeline writing CDC topics and the Flink job running:

```bash
docker compose stop taskmanager
```

Watch `kafka_consumergroup_lag` for `flink-stream-join` on **Kafka** or in Prometheus. After lag is above 200 and still climbing, **Consumer lag climbing** moves to pending or firing. Grafana **Pipeline health** lists the firing rule.

Start the taskmanager again with `docker compose start taskmanager`. The job may restore from checkpoint; lag should fall if the consumer resumes.


