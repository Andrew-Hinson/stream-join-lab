package com.streamjoin.model;

import java.time.Instant;
import org.apache.flink.table.data.RowData;
import org.junit.Test;
import static org.junit.Assert.*;

public class MatchFactsTest {
    @Test
    public void toRowData_utcAndNulls() {
        MatchFacts f = new MatchFacts();
        f.matchId = 42L;
        f.mapName = "Klyntar";
        f.matchDurationSeconds = 100;
        f.startedAt = Instant.parse("2026-01-15T12:00:00Z");
        f.endedAt = Instant.parse("2026-01-15T12:08:20Z");
        f.winningTeam = 1;
        f.playerId = 7L;
        f.accountName = "x";
        f.team = 0;
        f.heroPlayed = "Loki";
        f.kills = null;
        f.deaths = 3;
        f.healing = 10;
        f.result = "win";
        f.rankTierAtMatchStart = "bronze";
        f.rankDivisionAtMatchStart = 3;
        f.rankPointsAtMatchStart = 0;
        f.ingestedAt = Instant.parse("2026-01-15T13:00:00.123Z");

        RowData row = f.toRowData();
        assertEquals(42L, row.getLong(0));
        assertEquals("Klyntar", row.getString(1).toString());
        assertTrue(row.isNullAt(10));
        assertEquals(3, row.getInt(11));
        assertEquals(
            Instant.parse("2026-01-15T12:00:00Z"),
            row.getTimestamp(3, 3).toInstant());
    }
}