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

# Build the worker/monitor image (arm64, matching the "workers" Graviton node
# group) and push it to ECR, tagged with the current git SHA and `latest`.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=lib/eks_env.sh
source "${SCRIPT_DIR}/lib/eks_env.sh"

GIT_SHA="$(git -C "$ROOT" rev-parse --short HEAD)"
IMAGE_URI="${ECR_REPO_URI}:${GIT_SHA}"
IMAGE_URI_LATEST="${ECR_REPO_URI}:latest"

echo "Ensuring ECR repository ${ECR_REPO_NAME} exists in ${AWS_REGION} …"
aws ecr describe-repositories --region "$AWS_REGION" --repository-names "$ECR_REPO_NAME" \
  >/dev/null 2>&1 || aws ecr create-repository \
    --region "$AWS_REGION" \
    --repository-name "$ECR_REPO_NAME" \
    --image-scanning-configuration scanOnPush=true \
    --tags Key=project,Value=snowflake-elastic-channels >/dev/null

echo "Logging in to ECR …"
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

echo "Building ${IMAGE_URI} (linux/${WORKER_ARCH}) …"
docker buildx build \
  --platform "linux/${WORKER_ARCH}" \
  -f "${ROOT}/docker/Dockerfile" \
  -t "$IMAGE_URI" \
  -t "$IMAGE_URI_LATEST" \
  --push \
  "$ROOT"

echo "Pushed:"
echo "  ${IMAGE_URI}"
echo "  ${IMAGE_URI_LATEST}"
echo "$IMAGE_URI" > "${SCRIPT_DIR}/.last_image_uri"
