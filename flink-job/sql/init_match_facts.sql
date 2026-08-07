CREATE CATALOG lake WITH (
    'type'='iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 'file:///warehouse'
);

CREATE DATABASE IF NOT EXISTS lake.demo;


CREATE TABLE lake.demo.match_facts (
    match_id BIGINT,
    map_name STRING,
    match_duration_seconds INT,
    started_at TIMESTAMP(3),
    ended_at TIMESTAMP(3),
    winning_team INT,
    
    -- match_participants + players
    player_id BIGINT,
    account_name STRING,
    team INT,
    hero_played STRING,
    kills INT,
    deaths INT,
    healing INT,
    result STRING,

    -- ranks frozen at match time
    rank_tier_at_match_start STRING,
    rank_division_at_match_start INT,
    rank_points_at_match_start INT,

    -- Flink emit time
    ingested_at TIMESTAMP(3)
) WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);