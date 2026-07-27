#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." &&pwd)"
cd "$ROOT"
set -a
source "$ROOT/.env
uv run data_generator.py
echo "Exit code was: $?"