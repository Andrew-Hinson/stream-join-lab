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
        JsonNode a = after(json);
        if (a == null) return null;
        MatchEvent e = new MatchEvent();
        e.id = a.get("id").asLong();
        e.mapName = a.get("map_name").asText();
        e.matchDurationSeconds = a.get("match_duration_seconds").asInt();
        e.startedAt = Instant.parse(a.get("started_at").asText());
        e.endedAt = Instant.parse(a.get("ended_at").asText());
        e.winningTeam = a.get("winning_team").asInt();
        return e;
    }

    public static MatchParticipant parseMatchParticipant(String json) throws Exception {
        JsonNode a = after(json);
        if (a == null) return null;
        MatchParticipant p = new MatchParticipant();
        p.id = a.get("id").asLong();
        p.matchId = a.get("match_id").asLong();
        p.playerId = a.get("player_id").asLong();
        p.team = a.get("team").asInt();
        p.heroPlayed = a.get("hero_played").asText();
        p.kills = nullableInt(a, "kills");
        p.deaths = nullableInt(a, "deaths");
        p.healing = nullableInt(a, "healing");
        p.result = nullableText(a, "result");
        return p;
    }

    public static Player parsePlayer(String json) throws Exception {
        JsonNode a = after(json);
        if (a == null) return null;
        Player p = new Player();
        p.id = a.get("id").asLong();
        p.accountName = a.get("account_name").asText();
        p.email = a.get("email").asText();
        p.createdAt = Instant.parse(a.get("created_at").asText());
        p.accountStatus = a.get("account_status").asText();
        p.deletedAt = nullableInstant(a, "deleted_at");
        return p;
    }

    public static Rank parseRank(String json) throws Exception {
        JsonNode a = after(json);
        if (a == null) return null;
        Rank r = new Rank();
        r.playerId = a.get("player_id").asLong();
        r.rankTier = a.get("rank_tier").asText();
        r.rankDivision = a.get("rank_division").asInt();
        r.rankPoints = a.get("rank_points").asInt();
        r.wins = a.get("wins").asInt();
        r.losses = a.get("losses").asInt();
        r.updatedAt = Instant.parse(a.get("updated_at").asText());
        return r;
    }

    private static JsonNode after(String json) throws Exception {
        JsonNode a = MAPPER.readTree(json).get("payload").get("after");
        return (a == null || a.isNull()) ? null : a;
    }

    private static Integer nullableInt(JsonNode a, String field) {
        JsonNode n = a.get(field);
        return (n == null || n.isNull()) ? null : n.asInt();
    }

    private static String nullableText(JsonNode a, String field) {
        JsonNode n = a.get(field);
        return (n == null || n.isNull()) ? null : n.asText();
    }

    private static Instant nullableInstant(JsonNode a, String field) {
        JsonNode n = a.get(field);
        return (n == null || n.isNull()) ? null : Instant.parse(n.asText());
    }
}