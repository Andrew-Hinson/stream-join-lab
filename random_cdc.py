import argparse
from dataclasses import dataclass, field
import os
import uuid
from datetime import datetime, timezone

import psycopg
from faker import Faker

TABLE = "users"
COLUMNS = ("name", "email", "created_at", "is_active", "rank")

INSERT_SQL = f"""
    INSERT INTO {TABLE} ({", ".join(COLUMNS)})
    VALUES (%s, %s, %s, %s, %s)
"""

DELETE_SQL = f"""
    DELETE FROM {TABLE} WHERE id = %s
"""

RANKS = ["bronze", "silver", "gold", "platinum", "diamond"]


# In-memory table state
@dataclass
class TableState:
# every TableState instance could end up sharing the same set object. Adding an ID in one instance would show up in all of them.
# default_factory=set avoids that by running set() each time a new TableState() is created.
    ids: set = field(default_factory=set)

def generate_row(fake: Faker) -> tuple[str, str, datetime, bool, str]:
    return (
        fake.name(),
        f"{uuid.uuid4()}@example.com",
        fake.date_time_between(start_date="-2y", end_date="now", tzinfo=timezone.utc),
        fake.boolean(),
        fake.random_element(elements=RANKS),
    )

""" 
def update_row(current_row):
    fields = random.sample 
 """


def insert_rows(conn: psycopg.Connection, rows: list[tuple], batch_size: int) -> None:
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            cur.executemany(INSERT_SQL, rows[i : i + batch_size])

def delete_rows(conn: psycopg.Connection, rows: list[tuple], batch_size: int) -> None:
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            cur.executemany(DELETE_SQL, rows[i : i + batch_size])

def main() -> int:
    parser = argparse.ArgumentParser(description="Insert generated user data into PostgreSQL")
    parser.add_argument("--count", type=int, default=10, help="Number of rows to insert")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size for inserting rows")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
            "--database-url",
            default=os.getenv("DB_APP_WRITER_URL"),)
    
    args = parser.parse_args()
    


    fake = Faker()
    Faker.seed(args.seed)


    rows = [generate_row(fake) for _ in range(args.count)]
    
    try:
        with psycopg.connect(args.database_url) as conn:
            insert_rows(conn, rows, args.batch_size)
            conn.commit()
            print(f"Inserted {len(rows)} rows")             
    except psycopg.OperationalError as e:
        print(f"Error connecting to database: {e}")
        return 1
    
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())