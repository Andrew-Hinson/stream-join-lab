#!/bin/bash
set -a
source .env
set +a

read -p "This will delete ALL rows in 'users' table AND restart the sequence. Continue? (y/n): " confirm
if [[ "$confirm" != "y" ]]; then
    echo "Aborting."
    exit 1
fi

psql "$DB_SUPERUSER_URL" -c "TRUNCATE TABLE users RESTART IDENTITY;"
echo "Exit code: $?"