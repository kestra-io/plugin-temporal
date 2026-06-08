#!/usr/bin/env bash
set -euo pipefail

docker compose -f docker-compose-ci.yml down
