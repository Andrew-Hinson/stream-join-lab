import argparse
import os
from datetime import datetime, timezone

import psycopg

TABLE = "match_events"
COLUMNS = ("map_name", "match_duration_seconds", "started_at", "ended_at", "winning_team")

INSERT_SQL = f"""
    INSERT INTO {TABLE} ({", ".join(COLUMNS)})
    VALUES (%s, %s, %s, %s, %s)
"""

