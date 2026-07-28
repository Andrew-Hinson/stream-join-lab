## WIP

### Common Commands

Confirm connector is running: 
`curl -s http://localhost:8083/connectors/postgres-cdc/status | jq .`

List CDC Topics
`docker compose exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`