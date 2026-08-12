package com.streamjoin.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamjoin.model.MatchEvent;
import com.streamjoin.model.MatchParticipant;
import com.streamjoin.model.Player;
import com.streamjoin.model.Rank;

import java.time.Instant;

public class DebeziumEnvelope {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static MatchEvent parseMatchEvent(String json) throws Exception {
        JsonNode row = after(json);
        if (row == null) return null;
        MatchEvent e = new MatchEvent();
        e.id = row.get("id").asLong();
        e.mapName = row.get("map_name").asText();
        e.matchDurationSeconds = row.get("match_duration_seconds").asInt();
        e.startedAt = instant(row, "started_at");
        e.endedAt = instant(row, "ended_at");
        e.winningTeam = row.get("winning_team").asInt();
        return e;
    }

    public static MatchParticipant parseMatchParticipant(String json) throws Exception {
        JsonNode row = after(json);
        if (row == null) return null;
        MatchParticipant p = new MatchParticipant();
        p.id = row.get("id").asLong();
        p.matchId = row.get("match_id").asLong();
        p.playerId = row.get("player_id").asLong();
        p.team = row.get("team").asInt();
        p.heroPlayed = row.get("hero_played").asText();
        p.kills = nullableInt(row, "kills");
        p.deaths = nullableInt(row, "deaths");
        p.healing = nullableInt(row, "healing");
        p.result = nullableText(row, "result");
        return p;
    }

    public static Player parsePlayer(String json) throws Exception {
        JsonNode row = after(json);
        if (row == null) return null;
        Player p = new Player();
        p.id = row.get("id").asLong();
        p.accountName = row.get("account_name").asText();
        p.email = row.get("email").asText();
        p.createdAt = Instant.parse(row.get("created_at").asText());
        p.accountStatus = row.get("account_status").asText();
        p.deletedAt = nullableInstant(row, "deleted_at");
        return p;
    }

    public static Rank parseRank(String json) throws Exception {
        JsonNode row = after(json);
        if (row == null) return null;
        Rank r = new Rank();
        r.id = row.get("player_id").asLong();
        r.rankTier = row.get("rank_tier").asText();
        r.rankDivision = row.get("rank_division").asInt();
        r.rankPoints = row.get("rank_points").asInt();
        r.wins = row.get("wins").asInt();
        r.losses = row.get("losses").asInt();
        r.updatedAt = Instant.parse(row.get("updated_at").asText());
        return r;
    }

    private static JsonNode after(String json) throws Exception {
        JsonNode row = MAPPER.readTree(json).get("payload").get("after");
        return (row == null || row.isNull()) ? null : row;
    }

    private static Integer nullableInt(JsonNode row, String field) {
        JsonNode n = row.get(field);
        return (n == null || n.isNull()) ? null : n.asInt();
    }

    private static String nullableText(JsonNode row, String field) {
        JsonNode n = row.get(field);
        return (n == null || n.isNull()) ? null : n.asText();
    }

    private static Instant nullableInstant(JsonNode row, String field) {
        JsonNode n = row.get(field);
        return (n == null || n.isNull()) ? null : Instant.parse(n.asText());
    }

    private static Instant instant(JsonNode row, String field) {
        return Instant.parse(row.get(field).asText());
    }
}
