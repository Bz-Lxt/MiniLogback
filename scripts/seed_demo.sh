#!/bin/sh
set -eu

base_url="${MINILOGBACK_BASE_URL:-http://127.0.0.1:28640}"

curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d '{"events_per_second":25000,"duration_seconds":10,"payload_bytes":256}' \
  "${base_url}/api/v1/demo/traffic"
printf '\n'

curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d '{"size_bytes":512,"level":"error"}' \
  "${base_url}/api/v1/demo/leases"
printf '\n'

echo "Demo traffic and one intentionally retained lease were created."

