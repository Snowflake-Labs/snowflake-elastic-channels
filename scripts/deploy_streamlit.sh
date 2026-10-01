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

# Deploy Streamlit monitor to Snowflake (SPCS container runtime).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

snowflake_require() {
  command -v "$1" >/dev/null || { echo "Missing required command: $1" >&2; exit 1; }
}

snowflake_require snow

: "${SNOWFLAKE_ACCOUNT:?Set SNOWFLAKE_ACCOUNT in .env}"
: "${SNOWFLAKE_USER:?Set SNOWFLAKE_USER in .env}"
: "${SNOWFLAKE_PASSWORD:?Set SNOWFLAKE_PASSWORD in .env}"

STREAMLIT_WAREHOUSE="${SNOWFLAKE_STREAMLIT_WAREHOUSE:-LARGE_WH}"
# `snow` CLI flags like --account/--user/--password only *override* fields on
# an existing named connection profile (~/.snowflake/connections.toml) — they
# don't stand alone — so a profile matching this project's account must exist
# first. Override just the bits likely to drift between .env and the profile
# (role/warehouse/database/schema); account/user/password come from the
# profile itself.
SNOW_CONNECTION="${SNOWFLAKE_CLI_CONNECTION:-snowflake-elastic-channels}"

echo "Deploying ELASTIC_RAMP_MONITOR to ${SNOWFLAKE_ACCOUNT} (${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}) on ${STREAMLIT_WAREHOUSE} via connection '${SNOW_CONNECTION}'..."
unset SNOWFLAKE_PRIVATE_KEY_PATH SNOWFLAKE_PRIVATE_KEY

python_bin="${ROOT}/.venv/bin/python"
[[ -x "$python_bin" ]] || python_bin="python3"
"$python_bin" "${ROOT}/scripts/setup_streamlit_pypi.py" --app-role "${SNOWFLAKE_ROLE:-ACCOUNTADMIN}"

snow streamlit deploy elastic_ramp_monitor --replace --prune --project . \
  --connection "$SNOW_CONNECTION" \
  --role "${SNOWFLAKE_ROLE:-ACCOUNTADMIN}" \
  --warehouse "${STREAMLIT_WAREHOUSE}" \
  --database "${SNOWFLAKE_DATABASE:-SC_DB}" \
  --schema "${SNOWFLAKE_SCHEMA:-SC_SCHEMA}"

snow streamlit get-url elastic_ramp_monitor \
  --connection "$SNOW_CONNECTION" \
  --role "${SNOWFLAKE_ROLE:-ACCOUNTADMIN}" \
  --warehouse "${STREAMLIT_WAREHOUSE}" \
  --database "${SNOWFLAKE_DATABASE:-SC_DB}" \
  --schema "${SNOWFLAKE_SCHEMA:-SC_SCHEMA}"
