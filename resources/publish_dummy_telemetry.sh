#!/usr/bin/env bash
#
# Copyright 2026 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Publishes test telemetry to both local NATS cells. Local validation
# only; pairs with resources/local_multicell.yml and HOWTO.md.
#
# Two modes:
#
#   Dummy mode (default): randomized JSON payloads, one nats CLI process
#   per message. Good for validating end-to-end payload flow; capped at
#   a few dozen messages per second by the per-message process spawn.
#
#     resources/publish_dummy_telemetry.sh [count] [interval-seconds]
#       count             messages to publish (default 20)
#       interval-seconds  sleep between messages, fractional ok (default 0.2)
#
#   Bench mode: drives both cells concurrently with persistent
#   connections via `nats bench pub`. Good for throughput testing; the
#   payload is an opaque test pattern, not JSON. The per-cell message
#   budget is spread across a partitioned subject space: cell-a gets
#   telemetry.a.bench through telemetry.m.bench, cell-b gets
#   telemetry.n.bench through telemetry.z.bench, one publisher process
#   per subject.
#
#     resources/publish_dummy_telemetry.sh bench [msgs-per-cell] [size] [clients-per-subject]
#       msgs-per-cell       messages per cell, split across 13 subjects (default 100000)
#       size                message size, e.g. 256B or 1KB (default 256B)
#       clients-per-subject concurrent publishers per subject (default 1)

set -euo pipefail

CELL_NAMES=("cell-a" "cell-b")
CELL_URLS=("nats://localhost:4222" "nats://localhost:4223")

if ! command -v nats >/dev/null; then
  echo "error: the nats CLI is required (brew install nats-io/nats-tools/nats)" >&2
  exit 1
fi

if [ "${1:-}" = "bench" ]; then
  MSGS="${2:-100000}"
  SIZE="${3:-256B}"
  CLIENTS="${4:-1}"

  # subject partition: first-level token a-m goes to cell-a, n-z to cell-b
  CELL_A_LETTERS=(a b c d e f g h i j k l m)
  CELL_B_LETTERS=(n o p q r s t u v w x y z)
  per_subject=$((MSGS / ${#CELL_A_LETTERS[@]}))
  if [ "$per_subject" -lt 1 ]; then per_subject=1; fi
  total_per_cell=$((per_subject * ${#CELL_A_LETTERS[@]}))

  echo "benchmarking: ${total_per_cell} msgs per cell (${per_subject} x 13 subjects), ${SIZE} each, ${CLIENTS} client(s) per subject"
  echo "cell-a: telemetry.a.bench .. telemetry.m.bench"
  echo "cell-b: telemetry.n.bench .. telemetry.z.bench"

  start=$(date +%s)
  pids=()
  for letter in "${CELL_A_LETTERS[@]}"; do
    nats -s "${CELL_URLS[0]}" bench pub "telemetry.${letter}.bench" \
      --msgs "$per_subject" --size "$SIZE" --clients "$CLIENTS" --no-progress >/dev/null 2>&1 &
    pids+=($!)
  done
  for letter in "${CELL_B_LETTERS[@]}"; do
    nats -s "${CELL_URLS[1]}" bench pub "telemetry.${letter}.bench" \
      --msgs "$per_subject" --size "$SIZE" --clients "$CLIENTS" --no-progress >/dev/null 2>&1 &
    pids+=($!)
  done

  fail=0
  for pid in "${pids[@]}"; do
    wait "$pid" || fail=1
  done
  elapsed=$(($(date +%s) - start))
  if [ "$elapsed" -lt 1 ]; then elapsed=1; fi
  echo "published $((total_per_cell * 2)) msgs total in ${elapsed}s (~$(((total_per_cell * 2) / elapsed)) msgs/s aggregate)"
  exit "$fail"
fi

COUNT="${1:-20}"
INTERVAL="${2:-0.2}"

for ((i = 1; i <= COUNT; i++)); do
  cell=$((RANDOM % 2))
  car="car-$((RANDOM % 5 + 1))"
  speed=$((RANDOM % 130))
  soc=$((RANDOM % 101))
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  subject="telemetry.dummy.${car}"
  payload="$(printf '{"car_id":"%s","source_cell":"%s","seq":%d,"speed_kph":%d,"soc_pct":%d,"ts":"%s"}' \
    "$car" "${CELL_NAMES[$cell]}" "$i" "$speed" "$soc" "$ts")"

  nats -s "${CELL_URLS[$cell]}" pub "$subject" "$payload" 2>/dev/null
  echo "[$i/$COUNT] ${CELL_NAMES[$cell]} $subject $payload"
  sleep "$INTERVAL"
done
