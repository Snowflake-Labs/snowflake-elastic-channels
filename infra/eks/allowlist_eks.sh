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

# Allowlist this cluster's egress IP(s) in Snowflake. Worker/monitor pods run
# in private subnets and egress through the cluster VPC's NAT gateway(s), so
# there are only ever as many stable IPs as NAT gateways (one per AZ eksctl
# provisioned), and they don't change between runs. Re-run this once after
# cluster creation; no need to re-run per benchmark run.
#
# IPs are merged into this project's ELASTIC_RAMP_INGRESS rule (NAT gateways
# plus this machine's public IP). ELASTIC_RAMP_POLICY is created/updated but
# not attached. To enforce it, re-run setup_elastic_ramp.py with
# --attach-network-policy=user (or account, with the replacement acknowledgement).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=lib/eks_env.sh
source "${SCRIPT_DIR}/lib/eks_env.sh"

VPC_ID="$(aws eks describe-cluster --name "$CLUSTER_NAME" --region "$AWS_REGION" \
  --query 'cluster.resourcesVpcConfig.vpcId' --output text)"

echo "Cluster VPC: ${VPC_ID}"

NAT_IPS="$(aws ec2 describe-nat-gateways \
  --region "$AWS_REGION" \
  --filter "Name=vpc-id,Values=${VPC_ID}" "Name=state,Values=available" \
  --query 'NatGateways[].NatGatewayAddresses[].PublicIp' \
  --output text | tr '\t' ',' | tr ' ' ',')"

if [[ -z "$NAT_IPS" ]]; then
  echo "No NAT gateways found for VPC ${VPC_ID} — is the cluster up?" >&2
  exit 1
fi

echo "NAT gateway IP(s): ${NAT_IPS}"

ORCHESTRATOR_IP="$(curl -s https://checkip.amazonaws.com || true)"

cd "$ROOT"
python scripts/setup_elastic_ramp.py \
  --ingest-ips "$NAT_IPS" \
  ${ORCHESTRATOR_IP:+--orchestrator-ip "$ORCHESTRATOR_IP"}
