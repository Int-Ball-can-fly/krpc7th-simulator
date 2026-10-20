#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="$PROJECT_DIR/.env"

if [[ ! -f "$VENV_DIR/bin/activate" ]]; then
  echo "Python virtual environment not found: $VENV_DIR" >&2
  exit 1
fi

if [[ ! -f "$PROJECT_DIR/web/server.py" ]]; then
  echo "Web Manager entry point not found: $PROJECT_DIR/web/server.py" >&2
  exit 1
fi

cd "$PROJECT_DIR"
source "$VENV_DIR/bin/activate"
export PYTHONUNBUFFERED=1
exec python "$PROJECT_DIR/web/server.py"
