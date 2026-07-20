#!/bin/bash
set -a
source .env
set +a
uv run match_participants.py "$@"
echo "Exit code was: $?"