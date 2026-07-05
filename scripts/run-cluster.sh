#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="$ROOT/data"
mkdir -p "$DATA"

for id in 1 2 3; do
  client_port=$((9000 + id))
  raft_port=$((9100 + id))
  go run "$ROOT/cmd/server" \
    -id "$id" \
    -peers "1,2,3" \
    -addr ":$client_port" \
    -raft-addr ":$raft_port" \
    -peer-raft "1=localhost:9101,2=localhost:9102,3=localhost:9103" \
    -data "$DATA/node-$id" &
  echo "started node $id client=:$client_port raft=:$raft_port pid $!"
done

trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
wait
