import argparse
import os
import random
import time
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from datetime import datetime
import psycopg


MATCH_EVENTS_TABLE = "match_events"
MATCH_EVENTS_COLUMNS = ("map_name", "match_duration_seconds", "started_at", "ended_at", "winning_team")
INSERT_MATCH_EVENT_SQL = f"""
    INSERT INTO {MATCH_EVENTS_TABLE} ({", ".join(MATCH_EVENTS_COLUMNS)})
    VALUES (%s, %s, %s, %s, %s)
    RETURNING match_id;
"""


MATCH_PARTICIPANTS_TABLE = "match_participants"
MATCH_PARTICIPANTS_COLUMNS = ("match_id", "player_id", "team", "hero_played", "kills", "deaths", "healing", "result")
INSERT_MATCH_PARTICIPANT_SQL = f"""
    INSERT INTO {MATCH_PARTICIPANTS_TABLE} ({", ".join(MATCH_PARTICIPANTS_COLUMNS)})
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

SELECT_RANDOM_PLAYERS_SQL = f"""
    SELECT player_id FROM players
    ORDER BY random()
    LIMIT 12;
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
        cur.execute(SELECT_RANDOM_PLAYERS_SQL)
        rows = cur.fetchall()
    return [row[0] for row in rows]

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
    conn.commit()
    return match_id 

def build_match(player_ids: list[int]) -> MatchEvent:
    team_0, team_1, winning_team = assign_teams_and_result(player_ids)
    duration = random.randint(300, 1200)
    started_at = datetime.now(timezone.utc) - timedelta(seconds=duration)
    ended_at = datetime.now(timezone.utc)
    match = MatchEvent(
        map_name=random.choice(MAPS),
        duration=duration,
        started_at=started_at,
        ended_at=ended_at,
        winning_team=winning_team,
    )
    match.participants = build_participants(team_0, team_1, winning_team)
    return match
    
def simulate_match(conn: psycopg.Connection):
    player_ids = get_players(conn)
    if len(player_ids) < 12:
        print(f"Only {len(player_ids)} players found, need 12. Skipping.")
        return None
    
    match = build_match(player_ids)
    try:
        match_id = insert_match(conn, match)
        print(f"Inserted match {match_id} map={match.map_name} winning_team={match.winning_team}")
        return match_id
    except Exception as e:
        conn.rollback()
        print(f"Match insert failed, rolled back: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Simulating Marvel Rivals matches into Postgres")
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL")) 
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--min-sleep", type=float, default=1.0)
    parser.add_argument("--max-sleep", type=float, default=5.0)
    args = parser.parse_args()

    if not args.dsn:
        raise SystemExit("DATABASE_URL environment variable is required")
    
    conn = psycopg.connect(args.dsn)
    try:
        for i in range(args.count):
            simulate_match(conn)
            if i < args.count - 1:
                time.sleep(random.uniform(args.min_sleep,args.max_sleep))
    finally:
        conn.close()



if __name__ == "__main__":
    main()