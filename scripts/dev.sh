#!/usr/bin/env bash
# =============================================================================
# Quick dev launcher — runs backend + frontend dev servers in parallel.
# Use this from Git Bash on Windows or any Unix shell.
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -d .venv ]]; then
  echo "✗ .venv not found. Run scripts/setup.bat first."
  exit 1
fi

if [[ ! -f .env ]]; then
  echo "✗ .env not found."
  exit 1
fi

# shellcheck disable=SC1091
source .venv/Scripts/activate 2>/dev/null || source .venv/bin/activate

trap 'kill 0' EXIT INT TERM

echo "→ Starting backend on http://127.0.0.1:8080"
python -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8080 &

echo "→ Starting frontend on http://localhost:5173"
( cd frontend && npm run dev ) &

wait
