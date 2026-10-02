#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
if ! docker info >results/docker-info.log 2>&1; then
  echo 'Docker engine unavailable. Start Docker Desktop and enable WSL integration.' >&2
  exit 1
fi
# This project is a disposable PoC; reset only its own containers and volumes.
docker compose --profile test --profile fallback down -v --remove-orphans
rm -f results/observations.json results/report.md results/versions.json
after_exit() {
  code=$?
  docker compose --profile fallback logs --no-color >results/containers.log 2>&1 || true
  if [ "$code" -ne 0 ]; then echo 'FAILED; see results/containers.log' >&2; fi
}
trap after_exit EXIT
docker compose --profile test --profile fallback build
docker compose up -d --wait --wait-timeout 420 kafka cassandra connect
docker compose run --rm test experiments
docker compose --profile fallback up -d projection
docker compose run --rm test fallback
docker compose run --rm --entrypoint python test scripts/report.py
printf '%s\n' 'PASS: experiments and fallback. Evidence: results/observations.json'
