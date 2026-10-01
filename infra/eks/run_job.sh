#!/usr/bin/env bash
# Copyright 2026 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
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

# Run a fixed-throughput benchmark on EKS: one pod per worker, target bytes/sec
# split evenly across --workers pods, held for --duration-seconds. Ensures the
# worker node group has enough capacity, applies the Indexed Job, waits for
# completion, and prints where to find results (Snowflake ELASTIC_K8S_METRICS
# table — the monitor writes to it continuously — plus `kubectl logs` for raw
# worker output).
#
# Usage:
#   ./run_job.sh <run-id> --target-bps <aggregate_bytes_per_sec> \
#       --duration-seconds <seconds> [--workers 144] \
#       [--fast-json-encode true] [--keep-nodes]
#
# --fast-json-encode defaults to true (workers pre-encode NDJSON themselves and
# skip the SDK's redundant re-encode; falls back automatically to the SDK's
# default append path if unavailable, e.g. native wheel not installed). Pass
# --fast-json-encode false to disable and use the SDK's default path.
#
# --workers accepts any value from 1 to 455 (WORKER_COUNT_MIN/MAX in
# lib/eks_env.sh); the node group is scaled to fit automatically and back to
# zero when the run finishes.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/eks_env.sh
source "${SCRIPT_DIR}/lib/eks_env.sh"

RUN_ID="${1:?Usage: $0 <run-id> --target-bps N --duration-seconds N [--workers N] [--keep-nodes]}"
shift

TARGET_BPS=""
DURATION_SECONDS=""
WORKER_COUNT="${ELASTIC_RAMP_WORKERS:-144}"
MAX_BATCH_ROWS="80000"
TARGET_BATCH_BYTES="4194304"
MAX_IN_FLIGHT="64"
PIPE_NAME="${ELASTIC_PIPE_NAME:-RESORT_SALES_PIPE}"
FAST_JSON_ENCODE="true"
KEEP_NODES=0
IMAGE_URI=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-bps) TARGET_BPS="$2"; shift 2 ;;
    --duration-seconds) DURATION_SECONDS="$2"; shift 2 ;;
    --workers) WORKER_COUNT="$2"; shift 2 ;;
    --max-batch-rows) MAX_BATCH_ROWS="$2"; shift 2 ;;
    --target-batch-bytes) TARGET_BATCH_BYTES="$2"; shift 2 ;;
    --max-in-flight) MAX_IN_FLIGHT="$2"; shift 2 ;;
    --pipe-name) PIPE_NAME="$2"; shift 2 ;;
    --fast-json-encode) FAST_JSON_ENCODE="$2"; shift 2 ;;
    --image) IMAGE_URI="$2"; shift 2 ;;
    --keep-nodes) KEEP_NODES=1; shift ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

: "${TARGET_BPS:?--target-bps is required}"
: "${DURATION_SECONDS:?--duration-seconds is required}"

if (( WORKER_COUNT < WORKER_COUNT_MIN || WORKER_COUNT > WORKER_COUNT_MAX )); then
  echo "--workers must be between ${WORKER_COUNT_MIN} and ${WORKER_COUNT_MAX} (got ${WORKER_COUNT})" >&2
  exit 1
fi

if [[ -z "$IMAGE_URI" ]]; then
  if [[ -f "${SCRIPT_DIR}/.last_image_uri" ]]; then
    IMAGE_URI="$(cat "${SCRIPT_DIR}/.last_image_uri")"
  else
    IMAGE_URI="${ECR_REPO_URI}:latest"
  fi
fi

WORKER_TARGET_BPS=$(( TARGET_BPS / WORKER_COUNT ))
ACTIVE_DEADLINE_SECONDS=$(( DURATION_SECONDS + 300 ))

echo "Run ${RUN_ID}: workers=${WORKER_COUNT} aggregate_target=${TARGET_BPS} B/s " \
     "(~$(( WORKER_TARGET_BPS )) B/s/worker) duration=${DURATION_SECONDS}s " \
     "fast_json_encode=${FAST_JSON_ENCODE} image=${IMAGE_URI}"

"${SCRIPT_DIR}/scale_workers.sh" --workers "$WORKER_COUNT"

export RUN_ID WORKER_COUNT WORKER_TARGET_BPS DURATION_SECONDS ACTIVE_DEADLINE_SECONDS \
  MAX_BATCH_ROWS TARGET_BATCH_BYTES MAX_IN_FLIGHT PIPE_NAME FAST_JSON_ENCODE IMAGE_URI \
  WORKER_CPU_REQUEST WORKER_MEMORY_REQUEST

JOB_MANIFEST="/tmp/elastic-ramp-job-${RUN_ID}.yaml"
envsubst \
  '${RUN_ID} ${WORKER_COUNT} ${WORKER_TARGET_BPS} ${DURATION_SECONDS} ${ACTIVE_DEADLINE_SECONDS} ${MAX_BATCH_ROWS} ${TARGET_BATCH_BYTES} ${MAX_IN_FLIGHT} ${PIPE_NAME} ${FAST_JSON_ENCODE} ${IMAGE_URI} ${WORKER_CPU_REQUEST} ${WORKER_MEMORY_REQUEST}' \
  < "${SCRIPT_DIR}/manifests/worker-job.yaml.tmpl" > "$JOB_MANIFEST"

kubectl apply -f "$JOB_MANIFEST"

echo "Applied Job elastic-ramp-${RUN_ID}. Waiting up to ${ACTIVE_DEADLINE_SECONDS}s for completion…"
if kubectl -n "$ELASTIC_NAMESPACE" wait --for=condition=complete \
    "job/elastic-ramp-${RUN_ID}" --timeout="${ACTIVE_DEADLINE_SECONDS}s"; then
  echo "Job elastic-ramp-${RUN_ID} completed."
else
  echo "Job elastic-ramp-${RUN_ID} did not reach Complete within timeout; check status:" >&2
  kubectl -n "$ELASTIC_NAMESPACE" get job "elastic-ramp-${RUN_ID}" -o wide || true
  kubectl -n "$ELASTIC_NAMESPACE" get pods -l "run-id=${RUN_ID}" || true
fi

echo "Live/aggregated metrics: SELECT * FROM ${SNOWFLAKE_DATABASE:-SC_DB}.${SNOWFLAKE_SCHEMA:-SC_SCHEMA}.ELASTIC_K8S_METRICS WHERE RUN_ID = '${RUN_ID}' ORDER BY WINDOW_START;"
echo "Raw worker logs: kubectl -n ${ELASTIC_NAMESPACE} logs -l run-id=${RUN_ID} --prefix --tail=-1"

if [[ "$KEEP_NODES" -eq 0 ]]; then
  echo "Scaling worker node group back to 0 (pass --keep-nodes to skip)…"
  "${SCRIPT_DIR}/scale_workers.sh" --nodes 0
fi
