package com.streamjoin.model;

import java.time.Instant;

public class MatchEvent {
    public long id;
    public String mapName;
    public int matchDurationSeconds;
    public Instant startedAt;
    public Instant endedAt;
    public int winningTeam;
}
