package com.streamjoin.model;

import java.time.Instant;

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
}
