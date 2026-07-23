import argparse
import os
import uuid
from datetime import datetime, timezone

import psycopg
from faker import Faker

TABLE = "players"
COLUMNS = ("account_name", "email", "created_at", "account_status", "deleted_at")

INSERT_SQL = f"""
    INSERT INTO {TABLE} ({", ".join(COLUMNS)})
    VALUES (%s, %s, %s, %s, %s)
"""


def generate_row(fake: Faker) -> tuple[str, str, datetime, str, None]:
    return (
        fake.name(),
        f"{uuid.uuid4()}@example.com",
        fake.date_time_between(start_date="-2y", end_date="now", tzinfo=timezone.utc),
        "active",
        None,
    )


def insert_rows(conn: psycopg.Connection, rows: list[tuple]) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM players")
        count = cur.fetchone()
        if count is not None and count[0] >= 1000:
            print("Already seeded, skipping...")
            return
        else: 
            cur.executemany(INSERT_SQL, rows)
            print(f"Inserted {len(rows)} rows")   


def main() -> int:
    parser = argparse.ArgumentParser(description="Insert generated user data into PostgreSQL")
    parser.add_argument("--count", type=int, default=1000, help="Number of rows to insert")
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
            insert_rows(conn, rows)
            conn.commit()
            print(f"Inserted {len(rows)} rows")             
    except psycopg.OperationalError as e:
        print(f"Error connecting to database: {e}")
        return 1
    
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())