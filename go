#!/usr/bin/env bash
set -euo pipefail

# One-command launcher for the outreach pipeline.
exec python3 run_pipeline.py "$@"
