#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." &&pwd)"
cd "$ROOT"
set -a
source "$ROOT/.env"
set +a
uv run match_participants.py "$@"
echo "Exit code was: $?"