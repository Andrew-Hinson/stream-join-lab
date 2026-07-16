import argparse
import os
import random
from datetime import datetime, timezone
from dataclasses import dataclass
from datetime import datetime
import psycopg

from faker import Faker

MATCH_EVENTS_TABLE = "match_events"
MATCH_EVENTS_COLUMNS = ("map_name", "match_duration_seconds", "started_at", "ended_at", "winning_team")
INSERT_MATCH_EVENT_SQL = f"""
    INSERT INTO {MATCH_EVENTS_TABLE} ({", ".join(MATCH_EVENTS_COLUMNS)})
    VALUES (%s, %s, %s, %s, %s)
"""


MATCH_PARTICIPANTS_TABLE = "match_participants"
MATCH_PARTICIPANTS_COLUMNS = ("match_id", "player_id", "team", "hero_played", "kills", "deaths", "healing")
INSERT_MATCH_PARTICIPANT_SQL = f"""
    INSERT INTO {MATCH_PARTICIPANTS_TABLE} ({", ".join(MATCH_PARTICIPANTS_COLUMNS)})
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

SELECT_RANDOM_PLAYERS_SQL = f"""
    SELECT player_id FROM players
    ORDER BY random()
    LIMIT 12;
"""






@dataclass
class MatchParticipant:
    player_id: int
    team: int
    hero_played: str
    kills: int
    deaths: int
    healing: int

@dataclass
class Match_Event:
    map_name: str
    duration: int
    started_at: datetime
    ended_at: datetime
    winning_team: int
    participants: list[MatchParticipant]



def insert_match(conn, match: Match_Event):
    with conn.cursor() as cur:
        cur.execute(INSERT_MATCH_EVENT_SQL, match)
        match_id = cur.fetchone()[0]

        for p in match.participants:
            cur.execute(INSERT_MATCH_PARTICIPANT_SQL, (match_id, p.player_id, p.team, p.hero_played, p.kills, p.deaths, p.healing))
            conn.commit()




def assign_wins(players: list[str]):
    player_index = {}
    for player in players:
        random_num = random.randint(1, 12)
        while random_num in player_index:
            random_num = random.randint(1, 12)
        if random_num not in player_index:
            player_index[random_num] = player

    print(player_index)

    #if playernum even, assign win
    #if player num odd, assign loss

def get_players(conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute(SELECT_PLAYERS_SQL)
        return cur.fetchall()


if __name__ == "__main__":
    assign_wins(current_players)