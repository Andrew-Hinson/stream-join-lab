#!/bin/bash
set -a
source .env
set +a
uv run data_generator.py
echo "Exit code was: $?"