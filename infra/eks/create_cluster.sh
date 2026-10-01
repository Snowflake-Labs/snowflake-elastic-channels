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

# One-time (per account) setup of the persistent EKS cluster. Safe to re-run
# (eksctl create cluster is not idempotent for an existing cluster — this
# script checks first and skips creation if the cluster already exists, but
# always re-applies the namespace/RBAC manifests).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/eks_env.sh
source "${SCRIPT_DIR}/lib/eks_env.sh"

if eksctl get cluster --name "$CLUSTER_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
  echo "Cluster ${CLUSTER_NAME} already exists in ${AWS_REGION}; skipping eksctl create."
else
  echo "Creating EKS cluster ${CLUSTER_NAME} in ${AWS_REGION} (this takes ~15-20 minutes)…"
  eksctl create cluster -f "${SCRIPT_DIR}/cluster.yaml"
fi

aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$AWS_REGION"

kubectl apply -f "${SCRIPT_DIR}/manifests/namespace.yaml"
kubectl apply -f "${SCRIPT_DIR}/manifests/rbac.yaml"

echo "Cluster ready. Next steps:"
echo "  1. ./infra/eks/build_and_push.sh          # build+push worker/monitor image"
echo "  2. ./infra/eks/create_secret.sh            # create Snowflake credentials secret"
echo "  3. ./infra/eks/allowlist_eks.sh            # allowlist NAT gateway IP(s) in Snowflake"
echo "  4. ./infra/eks/deploy_monitor.sh           # deploy the (single, persistent) monitor"
echo "  5. ./infra/eks/run_job.sh <run-id> ...     # run a benchmark"
