#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." &&pwd)"
cd "$ROOT"
set -a
source "$ROOT/.env"
set +a


read -p "This will delete ALL rows in 'players' table AND restart the sequence. Continue? (y/n): " confirm
if [[ "$confirm" != "y" ]]; then
    echo "Aborting."
    exit 1
fi

psql "$DB_SUPERUSER_URL" -c "TRUNCATE TABLE players RESTART IDENTITY;"
echo "Exit code: $?"