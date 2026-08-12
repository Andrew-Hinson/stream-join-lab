package com.streamjoin.model;

import java.time.Instant;

public class Rank {
    public long id;
    public String rankTier;
    public int rankDivision;
    public int rankPoints;
    public int wins;
    public int losses;
    public Instant updatedAt;

    @Override
    public String toString() {
        return "Rank{playerId=" + id
                + ", tier=" + rankTier
                + ", div=" + rankDivision
                + ", pts=" +rankPoints
                + ", updatedAt=" + updatedAt + "}";
    }
}
