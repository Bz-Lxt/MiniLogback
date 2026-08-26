#!/bin/sh
set -eu

base_url="${MINILOGBACK_BASE_URL:-http://127.0.0.1:28640}"

curl --fail --silent --show-error "${base_url}/healthz" >/tmp/minilogback-health.json
curl --fail --silent --show-error "${base_url}/api/v1/metrics/current" >/tmp/minilogback-metrics.json
curl --fail --silent --show-error "${base_url}/api/v1/config/effective" >/tmp/minilogback-config.json
curl --fail --silent --show-error "${base_url}/" >/tmp/minilogback-index.html

grep -q '"status"' /tmp/minilogback-health.json
grep -q '"ring"' /tmp/minilogback-metrics.json
grep -q '"ring_capacity"' /tmp/minilogback-config.json
grep -qi '<!doctype html' /tmp/minilogback-index.html

echo "MiniLogback smoke test passed: ${base_url}"

