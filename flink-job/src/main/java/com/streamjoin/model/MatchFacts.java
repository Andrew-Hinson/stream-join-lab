package com.streamjoin.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;


public class MatchFacts {
    // match_events
    public long matchId;
    public String mapName;
    public int matchDurationSeconds;
    public Instant startedAt;
    public Instant endedAt;
    public int winningTeam;

    // match_participants + players
    public long playerId;
    public String accountName;
    public int team;
    public String heroPlayed;
    public Integer kills;
    public Integer deaths;
    public Integer healing;
    public String result;

    // ranks, as-of startedAt
    public String rankTierAtMatchStart;
    public Integer rankDivisionAtMatchStart;
    public Integer rankPointsAtMatchStart;

    // set by Flink at emit time
    public Instant ingestedAt;
    
    @Override
    public String toString() {
        return "MatchFacts{"        
        + "matchId=" + matchId
        + ", mapName=" + mapName
        + ", startedAt=" + startedAt
        + ", playerId=" + playerId
        + ", accountName=" + accountName
        + ", heroPlayed=" + heroPlayed
        + ", team=" + team
        + ", kills=" + kills
        + ", deaths=" + deaths    
        + ", healing=" + healing
        + ", result=" + result
        + ", rankTierAtMatchStart=" + rankTierAtMatchStart
        + ", rankDivisionAtMatchStart=" + rankDivisionAtMatchStart
        + ", rankPointsAtMatchStart=" + rankPointsAtMatchStart
        + ", ingestedAt=" + ingestedAt
        + ", matchDurationSeconds=" + matchDurationSeconds
        + ", winningTeam=" + winningTeam
        + ", endedAt=" + endedAt
        + "}";
    }
    
    public static void ensureTable() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("uri", envOr("ICEBERG_REST_URI", "http://iceberg-rest:8181"));
        props.put("warehouse", envOr("ICEBERG_WAREHOUSE", "s3://warehouse/"));
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", envOr("AWS_S3_ENDPOINT", "http://silo:9000"));
        props.put("s3.path-style-access", "true");
        props.put("s3.access-key-id", envOr("AWS_ACCESS_KEY_ID", "admin"));
        props.put("s3.secret-access-key", envOr("AWS_SECRET_ACCESS_KEY", "password"));
        props.put("client.region", envOr("AWS_REGION", "us-east-1"));
    
    
        Schema schema = new Schema( 
            Types.NestedField.optional(1, "match_id", Types.LongType.get()),
            Types.NestedField.optional(2, "map_name", Types.StringType.get()),
            Types.NestedField.optional(3, "match_duration_seconds", Types.IntegerType.get()),
            Types.NestedField.optional(4, "started_at", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(5, "ended_at", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(6, "winning_team", Types.IntegerType.get()),
            Types.NestedField.optional(7, "player_id", Types.LongType.get()),
            Types.NestedField.optional(8, "account_name", Types.StringType.get()),
            Types.NestedField.optional(9, "team", Types.IntegerType.get()),
            Types.NestedField.optional(10, "hero_played", Types.StringType.get()),
            Types.NestedField.optional(11, "kills", Types.IntegerType.get()),
            Types.NestedField.optional(12, "deaths", Types.IntegerType.get()),
            Types.NestedField.optional(13, "healing", Types.IntegerType.get()),
            Types.NestedField.optional(14, "result", Types.StringType.get()),
            Types.NestedField.optional(15, "rank_tier_at_match_start", Types.StringType.get()),
            Types.NestedField.optional(16, "rank_division_at_match_start", Types.IntegerType.get()),
            Types.NestedField.optional(17, "rank_points_at_match_start", Types.IntegerType.get()),
            Types.NestedField.optional(18, "ingested_at", Types.TimestampType.withoutZone()));
        
        Map<String, String> tableProps = new HashMap<>();
            tableProps.put("format-version", "2");
            tableProps.put("write.format.default", "parquet");

        RESTCatalog catalog = new RESTCatalog();
            catalog.initialize("lake", props);
        try {
            Namespace demo = Namespace.of("demo");
            if (!catalog.namespaceExists(demo)) {
                catalog.createNamespace(demo);
        }
        TableIdentifier table = TableIdentifier.of(demo, "match_facts");
        if (!catalog.tableExists(table)) {
            catalog.createTable(table, schema, PartitionSpec.unpartitioned(), tableProps);
        }
    } finally {
        catalog.close();
    }
    }
    private static String envOr(String key, String fallback) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? fallback : v;
    }
}
