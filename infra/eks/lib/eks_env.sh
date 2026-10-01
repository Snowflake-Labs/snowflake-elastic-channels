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

# Shared config for infra/eks/*.sh. Override any of these via the environment
# before calling a script (e.g. `AWS_REGION=us-east-1 ./create_cluster.sh`).
set -euo pipefail

export AWS_REGION="${AWS_REGION:-us-west-2}"
export CLUSTER_NAME="${CLUSTER_NAME:-snowflake-elastic-channels}"
export ELASTIC_NAMESPACE="${ELASTIC_NAMESPACE:-elastic-ramp}"
export ECR_REPO_NAME="${ECR_REPO_NAME:-snowflake-elastic-channels}"
# Per-worker-pod resources, sized against Graviton m7g.8xlarge (32 vCPU /
# 128GiB nominal; ~31.85 vCPU / ~120.7GiB allocatable after kubelet/system-
# reserved overhead, per `kubectl describe node`). CPU request/limit
# (1 vCPU) is set from measured steady-state usage with the default fast
# JSON encode path: ~240-250m at realistic (fleet-ceiling-constrained)
# per-worker rates, up to ~510-540m at an aggressive 400MB/s/worker synthetic
# rate, with negligible cgroup CPU throttling in both cases — well under the
# 1 vCPU limit. Memory request/limit (1Gi) is set from measured steady-state
# usage (observed 68-183Mi per pod across the tested rate range) with
# generous headroom. At these values CPU is the binding constraint —
# floor(31.85/1)=31 pods/node — so WORKERS_PER_NODE=31 is both the
# schedulable and the intended count; memory allows well over 100 pods/node,
# leaving no headroom on the table. See docs/architecture.md "EKS test flow".
export WORKER_INSTANCE_TYPE="${WORKER_INSTANCE_TYPE:-m7g.8xlarge}"
export WORKER_ARCH="${WORKER_ARCH:-arm64}"
export WORKERS_PER_NODE="${WORKERS_PER_NODE:-31}"
export SYSTEM_INSTANCE_TYPE="${SYSTEM_INSTANCE_TYPE:-m7g.large}"
export WORKER_NODEGROUP_NAME="${WORKER_NODEGROUP_NAME:-workers}"
export SYSTEM_NODEGROUP_NAME="${SYSTEM_NODEGROUP_NAME:-system}"
# Supported worker-count range for a single run (validated by run_job.sh).
export WORKER_COUNT_MIN="${WORKER_COUNT_MIN:-1}"
export WORKER_COUNT_MAX="${WORKER_COUNT_MAX:-455}"
# ASG max size for the workers node group — must fit WORKER_COUNT_MAX at
# WORKERS_PER_NODE (ceil(455/31)=15) and must match `workers.maxSize` in
# cluster.yaml (`eksctl scale nodegroup --nodes-max` can raise/lower an
# existing managed node group's ASG limits without recreating it, but the two
# should be kept in sync so a future `create_cluster.sh` on a fresh cluster
# gets the same ceiling).
export WORKER_NODEGROUP_MAX_NODES="${WORKER_NODEGROUP_MAX_NODES:-15}"
export WORKER_CPU_REQUEST="${WORKER_CPU_REQUEST:-1000m}"
export WORKER_MEMORY_REQUEST="${WORKER_MEMORY_REQUEST:-1Gi}"

AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export AWS_ACCOUNT_ID
export ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
export ECR_REPO_URI="${ECR_REGISTRY}/${ECR_REPO_NAME}"
