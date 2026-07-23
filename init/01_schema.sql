CREATE TABLE players (
    id SERIAL PRIMARY KEY,
    account_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPZ NOT NULL DEFAULT now()
    account_status TEXT NOT NULL DEFAULT 'active',
    deleted_at TIMESTAMPZ,
);

CREATE TABLE ranks (
    player_id INTEGER PRIMARY KEY REFERENCES players(id),
    rank_tier TEXT NOT NULL,
    rank_division SMALLINT NOT NULL,
    rank_points SMALLINT NOT NULL,
    wins INTEGER NOT NULL DEFAULT 0,
    losses INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPZ NOT NULL DEFAULT now()
);

CREATE TABLE match_events (
    id SERIAL PRIMARY KEY,
    map_name TEXT,
    match_duration_seconds INTEGER NOT NULL,
    started_at TIMESTAMPZ NOT NULL,
    ended_at TIMESTAMPZ NOT NULL,
    winning_team SMALLINT NOT NULL
);

CREATE TABLE match_participants (
    id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES match_events(id),
    player_id INTEGER NOT NULL REFERENCES players(id),
    team SMALLINT NOT NULL,          -- 0 or 1
    hero_played TEXT NOT NULL,
    kills INTEGER,
    deaths INTEGER,
    healing INTEGER,
    result TEXT
);