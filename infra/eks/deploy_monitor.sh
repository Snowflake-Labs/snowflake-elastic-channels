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

# (Re)deploy the single persistent monitor Deployment with the latest built
# image. Run once after cluster creation, and again whenever k8s_monitor.py
# changes and you rebuild the image.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/eks_env.sh
source "${SCRIPT_DIR}/lib/eks_env.sh"

IMAGE_URI="${1:-}"
if [[ -z "$IMAGE_URI" ]]; then
  if [[ -f "${SCRIPT_DIR}/.last_image_uri" ]]; then
    IMAGE_URI="$(cat "${SCRIPT_DIR}/.last_image_uri")"
  else
    IMAGE_URI="${ECR_REPO_URI}:latest"
  fi
fi

echo "Deploying monitor with image ${IMAGE_URI} …"
export IMAGE_URI
envsubst '${IMAGE_URI}' < "${SCRIPT_DIR}/manifests/monitor-deployment.yaml.tmpl" \
  | kubectl apply -f -

# build_and_push.sh tags by git SHA, which is unchanged by local (uncommitted)
# edits — `kubectl apply` then sees no spec diff and skips the rollout even
# though the image content changed. Force a restart every time so this script
# always ends up running whatever was just pushed (imagePullPolicy: Always
# ensures the digest, not just the tag, is re-checked).
kubectl -n "$ELASTIC_NAMESPACE" rollout restart deployment/elastic-monitor
kubectl -n "$ELASTIC_NAMESPACE" rollout status deployment/elastic-monitor --timeout=120s
