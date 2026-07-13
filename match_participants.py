import argparse
import os
import random
from datetime import datetime, timezone

import psycopg

from faker import Faker

TABLE = "match_players"
COLUMNS = ("match_id", "player_id", "team", "hero_played", "kills", "deaths", "healing", "result")

SELECT_PLAYERS_SQL = f"""
    SELECT player_id FROM players
    ORDER BY random()
    LIMIT 12;
"""
current_players = [
    "NeonVandal",
    "QuietPixel",
    "IronMoth",
    "GlitchFox",
    "ZeroComet",
    "AshCircuit",
    "LunarBite",
    "StaticWolf",
    "VoidRanger",
    "EmberKnob",
    "FrostByteX",
    "CrimsonPing",
]

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