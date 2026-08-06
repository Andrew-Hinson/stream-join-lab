CREATE CATALOG lake WITH (
    'type'='iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 'file:///warehouse'
);

CREATE DATABASE IF NOT EXISTS lake.demo;


CREATE TABLE lake.demo.match_facts (
    match_id BIGINT,
    player_id BIGINT,
    map_name STRING,
    hero_played STRING,
    team INT,
    kills INT,
    deaths INT,
    result STRING,
    rank_tier STRING,
    started_at TIMESTAMP(3)
) WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);