#!/usr/bin/env bash
set -euo pipefail

docker compose -f docker-compose-ci.yml up -d --wait
echo "Temporal dev server ready on localhost:7233"
