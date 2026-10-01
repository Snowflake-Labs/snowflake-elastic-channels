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

# Scale the "workers" managed node group. Cheap way to add/remove capacity
# between runs without touching the cluster control plane or the "system"
# node group (monitor keeps running throughout).
#
# Usage:
#   ./scale_workers.sh --workers 144      # size for N worker pods (rounds up nodes)
#   ./scale_workers.sh --nodes 9          # size by node count directly
#   ./scale_workers.sh --workers 0        # scale to zero (teardown_workers.sh does this too)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/eks_env.sh
source "${SCRIPT_DIR}/lib/eks_env.sh"

NODES=""
WORKERS=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --workers) WORKERS="$2"; shift 2 ;;
    --nodes) NODES="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$NODES" ]]; then
  if [[ -z "$WORKERS" ]]; then
    echo "Usage: $0 --workers N | --nodes N" >&2
    exit 1
  fi
  if [[ "$WORKERS" -eq 0 ]]; then
    NODES=0
  else
    NODES=$(( (WORKERS + WORKERS_PER_NODE - 1) / WORKERS_PER_NODE ))
  fi
fi

echo "Scaling node group ${WORKER_NODEGROUP_NAME} to ${NODES} node(s) (${WORKERS_PER_NODE} workers/node)…"
eksctl scale nodegroup \
  --cluster "$CLUSTER_NAME" \
  --region "$AWS_REGION" \
  --name "$WORKER_NODEGROUP_NAME" \
  --nodes "$NODES" \
  --nodes-min 0 \
  --nodes-max "$WORKER_NODEGROUP_MAX_NODES"

if [[ "$NODES" -gt 0 ]]; then
  echo "Waiting for ${NODES} worker node(s) to become Ready…"
  for _ in $(seq 1 60); do
    ready=$(kubectl get nodes -l role=elastic-ramp-worker --no-headers 2>/dev/null \
      | awk '$2=="Ready"' | wc -l | tr -d ' ')
    if [[ "$ready" -ge "$NODES" ]]; then
      echo "OK: ${ready}/${NODES} worker nodes Ready"
      exit 0
    fi
    sleep 10
  done
  echo "WARN: timed out waiting for worker nodes to become Ready (continuing anyway)" >&2
fi
