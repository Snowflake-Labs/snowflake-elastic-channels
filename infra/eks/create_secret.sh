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

# Create/update the `snowflake-credentials` Kubernetes Secret from the local
# .env — mirrors auth.py's env-var contract exactly so both worker and
# monitor pods can call ingest_client_properties()/connector_config()
# unmodified. Never writes the private key or password to disk inside the
# repo; reads .env directly and pipes into `kubectl apply` server-side.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=lib/eks_env.sh
source "${SCRIPT_DIR}/lib/eks_env.sh"

ENV_FILE="${1:-${ROOT}/.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Env file not found: $ENV_FILE" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

: "${SNOWFLAKE_ACCOUNT:?SNOWFLAKE_ACCOUNT not set in $ENV_FILE}"
: "${SNOWFLAKE_USER:?SNOWFLAKE_USER not set in $ENV_FILE}"

PRIVATE_KEY_PEM=""
if [[ -n "${SNOWFLAKE_PRIVATE_KEY:-}" ]]; then
  PRIVATE_KEY_PEM="$SNOWFLAKE_PRIVATE_KEY"
elif [[ -n "${SNOWFLAKE_PRIVATE_KEY_PATH:-}" ]]; then
  PRIVATE_KEY_PEM="$(cat "${SNOWFLAKE_PRIVATE_KEY_PATH/#\~/$HOME}")"
else
  echo "Set SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH in $ENV_FILE (EKS workers require key-pair auth)" >&2
  exit 1
fi

kubectl create namespace "$ELASTIC_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic snowflake-credentials \
  --namespace "$ELASTIC_NAMESPACE" \
  --from-literal=SNOWFLAKE_ACCOUNT="$SNOWFLAKE_ACCOUNT" \
  --from-literal=SNOWFLAKE_USER="$SNOWFLAKE_USER" \
  --from-literal=SNOWFLAKE_ROLE="${SNOWFLAKE_ROLE:-}" \
  --from-literal=SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-SC_DB}" \
  --from-literal=SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA:-SC_SCHEMA}" \
  --from-literal=SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-XSMALL_WH}" \
  --from-literal=SNOWFLAKE_PRIVATE_KEY="$PRIVATE_KEY_PEM" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "OK: secret/snowflake-credentials in namespace ${ELASTIC_NAMESPACE}"
