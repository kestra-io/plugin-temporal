#!/usr/bin/env bash
# Stops the Temporal dev server started by setup-unit.sh.
set -euo pipefail

docker compose -f docker-compose-ci.yml down
