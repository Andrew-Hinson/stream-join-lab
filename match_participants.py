import argparse
import os
import random
import time
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from datetime import datetime
import psycopg

DEFAULT_TIER, DEFAULT_DIV, DEFAULT_PTS = "bronze", 3, 0
WIN_DELTA, LOSS_DELTA = 30, -15

TIERS = ["bronze", "silver", "gold", "platinum", "diamond"]
PTS_PER_DIVISION = 100
DIVISIONS_PER_TIER = 5
PTS_PER_TIER = PTS_PER_DIVISION * DIVISIONS_PER_TIER

MATCH_EVENTS_TABLE = "match_events"
MATCH_EVENTS_COLUMNS = ("map_name", "match_duration_seconds", "started_at", "ended_at", "winning_team")
INSERT_MATCH_EVENT_SQL = f"""
    INSERT INTO {MATCH_EVENTS_TABLE} ({", ".join(MATCH_EVENTS_COLUMNS)})
    VALUES (%s, %s, %s, %s, %s)
    RETURNING id;
"""


MATCH_PARTICIPANTS_TABLE = "match_participants"
MATCH_PARTICIPANTS_COLUMNS = ("match_id", "player_id", "team", "hero_played", "kills", "deaths", "healing", "result")
INSERT_MATCH_PARTICIPANT_SQL = f"""
    INSERT INTO {MATCH_PARTICIPANTS_TABLE} ({", ".join(MATCH_PARTICIPANTS_COLUMNS)})
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


RANKS_TABLE = "ranks"
RANKS_COLUMNS = ("player_id", "rank_tier", "rank_division", "rank_points", "wins", "losses", "updated_at")
INSERT_RANKS_SQL = f"""
    INSERT INTO {RANKS_TABLE} ({", ".join(RANKS_COLUMNS)})
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (player_id) DO UPDATE SET
        rank_tier = EXCLUDED.rank_tier,
        rank_division = EXCLUDED.rank_division,
        rank_points = EXCLUDED.rank_points,
        wins = EXCLUDED.wins,
        losses = EXCLUDED.losses,
        updated_at = EXCLUDED.updated_at
"""
    
SELECT_RANKS_SQL = f"""
    SELECT rank_tier, rank_division, rank_points, wins, losses 
    FROM {RANKS_TABLE}
    WHERE player_id = %s
"""




MAPS = [
    "Tokyo 2099", "Yggsgard: Royal Palace", "Klyntar", "Intergalactic Empire of Wakanda",
    "Hell's Heaven", "Hydra Charteris Base", "Empire of Eternal Night",
]

HEROES = [
    "Adam Warlock",
    "Angela",
    "Black Cat",
    "Black Panther",
    "Black Widow",
    "Blade",
    "Captain America",
    "Cloak & Dagger",
    "Cyclops",
    "Daredevil",
    "Deadpool",
    "Devil Dinosaur",
    "Doctor Strange",
    "Elsa Bloodstone",
    "Emma Frost",
    "Gambit",
    "Groot",
    "Hawkeye",
    "Hela",
    "Hulk",
    "Human Torch",
    "Invisible Woman",
    "Iron Fist",
    "Iron Man",
    "Jeff the Land Shark",
    "Jubilee",
    "Loki",
    "Luna Snow",
    "Magik",
    "Magneto",
    "Mantis",
    "Mister Fantastic",
    "Moon Knight",
    "Namor",
    "Peni Parker",
    "Phoenix",
    "Psylocke",
    "Rocket Raccoon",
    "Rogue",
    "Scarlet Witch",
    "Spider-Man",
    "Squirrel Girl",
    "Star-Lord",
    "Storm",
    "The Punisher",
    "The Thing",
    "Thor",
    "Ultron",
    "Venom",
    "White Fox",
    "Winter Soldier",
    "Wolverine",
]



@dataclass
class MatchParticipant:
    player_id: int
    team: int
    hero_played: str
    kills: int
    deaths: int
    healing: int
    result: str

@dataclass
class MatchEvent:
    map_name: str
    duration: int
    started_at: datetime
    ended_at: datetime
    winning_team: int
    participants: list = field(default_factory=list)

def get_players(conn: psycopg.Connection) -> list[int]:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM players")
        return [row[0] for row in cur.fetchall()]

def assign_teams_and_result(players: list):
    shuffled = players[:]
    random.shuffle(shuffled)
    team_0, team_1 = shuffled[:6], shuffled[6:]
    winning_team = random.choice([0, 1])
    return team_0, team_1, winning_team


def build_participants(team_0, team_1, winning_team):
    participants = []
    for team_num, roster in ((0, team_0), (1, team_1)):
        result = "win" if team_num == winning_team else "loss"
        for player_id in roster:
            participants.append(MatchParticipant(
                player_id=player_id,
                team=team_num,
                hero_played=random.choice(HEROES),
                kills=random.randint(0, 40),
                deaths=random.randint(0, 20),
                healing=random.randint(0, 10000),
                result=result,
            ))
    return participants

def insert_match(conn, match: MatchEvent) -> int:
    with conn.cursor() as cur:
        cur.execute(
            INSERT_MATCH_EVENT_SQL, 
        (   match.map_name,
            match.duration,
            match.started_at,
            match.ended_at,
            match.winning_team,
        ),
        )

        match_id = cur.fetchone()[0]

        cur.executemany(
            INSERT_MATCH_PARTICIPANT_SQL,
            [
                (match_id, p.player_id, p.team, p.hero_played, p.kills, p.deaths, p.healing, p.result)
                for p in match.participants
            ], 
        )
        for p in match.participants:
            apply_rank(cur, p.player_id, p.result, match.ended_at)
    conn.commit()
    return match_id 

def build_match(player_ids: list[int], now_sim: datetime) -> MatchEvent:
    team_0, team_1, winning_team = assign_teams_and_result(player_ids)
    duration = random.randint(300, 1200)
    started_at = now_sim 
    ended_at = now_sim + timedelta(seconds=duration)

    match = MatchEvent(
        map_name=random.choice(MAPS),
        duration=duration,
        started_at=started_at,
        ended_at=ended_at,
        winning_team=winning_team,
    )
    match.participants = build_participants(team_0, team_1, winning_team)
    return match
    
def apply_rank(cur, player_id: int, result: str, updated_at: datetime):
    cur.execute(SELECT_RANKS_SQL, (player_id,))
    row = cur.fetchone()
    if row is None:
        print(f"Player {player_id} has not played any matches yet")
        tier, div, pts, wins, losses = DEFAULT_TIER, DEFAULT_DIV, DEFAULT_PTS, 0, 0
    else:
        tier, div, pts, wins, losses = row
    if result == "win":
        pts = max(0, pts + WIN_DELTA)
        wins += 1
    else:
        pts = max(0, pts + LOSS_DELTA)
        losses += 1
    tier, div = derive_tier_and_division(pts)

    cur.execute(
        INSERT_RANKS_SQL,
        (player_id, tier, div, pts, wins, losses, updated_at),
    )


def derive_tier_and_division(pts: int) -> tuple[str, int]:
    pts = max(0, pts)
    max_pts = PTS_PER_TIER * len(TIERS) - 1
    pts = min(pts, max_pts)

    tier_idx = pts // PTS_PER_TIER
    within = pts % PTS_PER_TIER

    division = DIVISIONS_PER_TIER - (within // PTS_PER_DIVISION)
    return TIERS[tier_idx], division
    
def take_roster(free: set[int], n: int = 12) -> list[int]:
    if len(free) < n:
        return []
    roster = random.sample(tuple(free), n)
    free.difference_update(roster)
    return roster

def occupy_roster(busy: dict, roster: list[int], ended_at: datetime) -> None:
    for pid in roster:
        busy[pid] = ended_at

def release_roster(free: set[int], busy: dict, roster: list[int]) -> None:
    for pid in roster:
        busy.pop(pid, None)
        free.add(pid)

def start_match(free, busy, now_sim) -> MatchEvent | None:
    roster = take_roster(free)
    if not roster:
        return None
    match = build_match(roster, now_sim)
    occupy_roster(busy, roster, match.ended_at)
    return match

def finish_match (conn, free, busy, match):
    roster = [p.player_id for p in match.participants]
    try:
        match_id = insert_match(conn, match)
        release_roster(free, busy, roster)
        return match_id
    except Exception as e:
        release_roster(free, busy, roster)
        conn.rollback()
        print(f"Match insert failed, rolled back: {e}")
        return None
        
def main():
    parser = argparse.ArgumentParser(description="Simulating Marvel Rivals matches into Postgres")
    parser.add_argument("--dsn", default=os.environ.get("DB_APP_WRITER_URL")) 
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--min-sleep", type=float, default=1.0)
    parser.add_argument("--max-sleep", type=float, default=5.0)
    args = parser.parse_args()

    if not args.dsn:
        raise SystemExit("DATABASE_URL environment variable is required")
    
    conn = psycopg.connect(args.dsn)
    free: set[int] = set(get_players(conn))
    if len(free) < 12:
        raise SystemExit(f"Need 12 players, got {len(free)}")
    busy: dict[int, datetime] = {}
    now_sim = datetime.now(timezone.utc)
    in_flight: list[MatchEvent] = []
    inserted = 0
    try:
        while inserted < args.count:
            while len(free) >= 12 and inserted + len(in_flight) < args.count:
                match = start_match(free, busy, now_sim)
                if match is None:
                    break
                in_flight.append(match)
            if not in_flight:
                break
            
            in_flight.sort(key=lambda m: m.ended_at)
            done = in_flight.pop(0)
            now_sim = done.ended_at
            finish_match(conn, free, busy, done)
            inserted += 1
            if inserted < args.count:
                time.sleep(random.uniform(args.min_sleep, args.max_sleep)) 
    finally:
        conn.close()



if __name__ == "__main__":
    main()