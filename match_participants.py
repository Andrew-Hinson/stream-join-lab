import argparse
import heapq
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
import psycopg
TIERS = ["bronze", "silver", "gold", "platinum", "diamond"]
PTS_PER_DIV, DIVS_PER_TIER = 100, 5
PTS_PER_TIER = PTS_PER_DIV * DIVS_PER_TIER
WIN_DELTA, LOSS_DELTA = 30, -15
TEAM_SIZE = 6
ROSTER_SIZE = TEAM_SIZE * 2
MAPS = [
    "Tokyo 2099", "Yggsgard: Royal Palace", "Klyntar",
    "Intergalactic Empire of Wakanda", "Hell's Heaven",
    "Hydra Charteris Base", "Empire of Eternal Night",
]
HEROES = [
    "Adam Warlock", "Angela", "Black Cat", "Black Panther", "Black Widow",
    "Blade", "Captain America", "Cloak & Dagger", "Cyclops", "Daredevil",
    "Deadpool", "Devil Dinosaur", "Doctor Strange", "Elsa Bloodstone",
    "Emma Frost", "Gambit", "Groot", "Hawkeye", "Hela", "Hulk", "Human Torch",
    "Invisible Woman", "Iron Fist", "Iron Man", "Jeff the Land Shark",
    "Jubilee", "Loki", "Luna Snow", "Magik", "Magneto", "Mantis",
    "Mister Fantastic", "Moon Knight", "Namor", "Peni Parker", "Phoenix",
    "Psylocke", "Rocket Raccoon", "Rogue", "Scarlet Witch", "Spider-Man",
    "Squirrel Girl", "Star-Lord", "Storm", "The Punisher", "The Thing",
    "Thor", "Ultron", "Venom", "White Fox", "Winter Soldier", "Wolverine",
]

@dataclass(order=True)
class Match:
    ended_at: datetime
    map_name: str = field(compare=False)
    duration: int = field(compare=False)
    started_at: datetime = field(compare=False)
    winning_team: int = field(compare=False)
    participants: list = field(default_factory=list, compare=False)

def derive_rank(pts: int) -> tuple[str, int]:
    pts = max(0, min(pts, PTS_PER_TIER * len(TIERS) - 1))
    tier_idx, within = divmod(pts, PTS_PER_TIER)
    return TIERS[tier_idx], DIVS_PER_TIER - within // PTS_PER_DIV


def build_match(free: set[int], started_at: datetime) -> Match:
    roster = random.sample(tuple(free), ROSTER_SIZE)
    free.difference_update(roster)
    winning_team = random.choice([0, 1])
    duration = random.randint(300, 1200)
    match = Match(
        ended_at=started_at + timedelta(seconds=duration),
        map_name=random.choice(MAPS),
        duration=duration,
        started_at=started_at,
        winning_team=winning_team,
    )
    for i, pid in enumerate(roster):
        team = 0 if i < TEAM_SIZE else 1
        match.participants.append({
            "player_id": pid,
            "team": team,
            "hero": random.choice(HEROES),
            "kills": random.randint(0, 40),
            "deaths": random.randint(0, 20),
            "healing": random.randint(0, 10000),
            "result": "win" if team == winning_team else "loss",
        })
    return match

def update_rank(cur, pid: int, result: str, updated_at: datetime) -> None:
    cur.execute(
        "SELECT rank_points, wins, losses FROM ranks WHERE player_id = %s",
        (pid,),
    )
    row = cur.fetchone()
    pts, wins, losses = row if row else (0, 0, 0)
    won = result == "win"
    pts += WIN_DELTA if won else LOSS_DELTA
    wins += int(won)
    losses += int(not won)
    tier, div = derive_rank(pts)
    cur.execute(
        """
        INSERT INTO ranks (player_id, rank_tier, rank_division, rank_points,
                           wins, losses, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (player_id) DO UPDATE SET
            rank_tier = EXCLUDED.rank_tier,
            rank_division = EXCLUDED.rank_division,
            rank_points = EXCLUDED.rank_points,
            wins = EXCLUDED.wins,
            losses = EXCLUDED.losses,
            updated_at = EXCLUDED.updated_at
        """,
        (pid, tier, div, max(0, pts), wins, losses, updated_at),
    )
    

def insert_match(conn, match: Match) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO match_events (map_name, match_duration_seconds,
                                      started_at, ended_at, winning_team)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
            """,
            (match.map_name, match.duration, match.started_at,
             match.ended_at, match.winning_team),
        )
        match_id = cur.fetchone()[0]
        cur.executemany(
            """
            INSERT INTO match_participants
                (match_id, player_id, team, hero_played, kills, deaths, healing, result)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [(match_id, p["player_id"], p["team"], p["hero"], p["kills"],
              p["deaths"], p["healing"], p["result"]) for p in match.participants],
        )
        for p in match.participants:
            update_rank(cur, p["player_id"], p["result"], match.ended_at)
    conn.commit()
    
def main():
    parser = argparse.ArgumentParser(description="Simulate Marvel Rivals matches into Postgres")
    parser.add_argument("--dsn", default=os.environ.get("DB_APP_WRITER_URL"))
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--min-sleep", type=float, default=1.0)
    parser.add_argument("--max-sleep", type=float, default=5.0)
    args = parser.parse_args()
    if not args.dsn:
        raise SystemExit("DATABASE_URL is required")
    with psycopg.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM players")
            free = {r[0] for r in cur.fetchall()}
        if len(free) < ROSTER_SIZE:
            raise SystemExit(f"Need {ROSTER_SIZE} players, got {len(free)}")
        now_sim = datetime.now(timezone.utc)
        in_flight: list[Match] = []
        inserted = 0
        while inserted < args.count:
            while len(free) >= ROSTER_SIZE and inserted + len(in_flight) < args.count:
                heapq.heappush(in_flight, build_match(free, now_sim))
            if not in_flight:
                break
            done = heapq.heappop(in_flight)
            now_sim = done.ended_at
            free.update(p["player_id"] for p in done.participants)
            try:
                insert_match(conn, done)
                inserted += 1
            except Exception as e:
                conn.rollback()
                print(f"Insert failed: {e}")
                continue
            if inserted < args.count:
                time.sleep(random.uniform(args.min_sleep, args.max_sleep))
if __name__ == "__main__":
    main()