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

# Fully delete the EKS cluster (control plane + all node groups). Rarely
# needed given the cluster is meant to be persistent — use
# ./teardown_workers.sh between runs instead. Requires explicit confirmation.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/eks_env.sh
source "${SCRIPT_DIR}/lib/eks_env.sh"

read -r -p "Delete EKS cluster '${CLUSTER_NAME}' in ${AWS_REGION} entirely? [y/N] " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
  echo "Aborted."
  exit 1
fi

eksctl delete cluster --name "$CLUSTER_NAME" --region "$AWS_REGION"
