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

# Render docs/architecture.png from docs/architecture.dot (Graphviz).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DOT="${ROOT}/docs/architecture.dot"
PNG="${ROOT}/docs/architecture.png"

if ! command -v dot >/dev/null 2>&1; then
  echo "Graphviz 'dot' is required. Install: brew install graphviz" >&2
  exit 1
fi

if [[ ! -f "$DOT" ]]; then
  echo "Missing DOT source: $DOT" >&2
  exit 1
fi

dot -Tpng "$DOT" -o "$PNG"
echo "Wrote ${PNG}"
