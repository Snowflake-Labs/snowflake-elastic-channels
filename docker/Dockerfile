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

# Single image for both the EKS worker (k8s_worker.py) and monitor
# (k8s_monitor.py) entrypoints — selected via each pod's `command`/`args`
# (see infra/eks/manifests/). Built for arm64 (Graviton node groups, matching
# the EC2 path's c9g instances) via `infra/eks/build_and_push.sh`.

# Stage 1: build the native/ NDJSON-encoder wheel (see
# elastic_channels/resort_sales.py, fast_append.py), used by default at
# runtime (ELASTIC_FAST_JSON_ENCODE=true) with no rebuild needed to change
# it; the Python code falls back cleanly to msgspec if this stage's wheel
# were ever unavailable.
FROM python:3.13-bookworm AS native-builder

WORKDIR /build/native
RUN apt-get update && apt-get install -y --no-install-recommends curl build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable \
    && pip install --no-cache-dir maturin
ENV PATH="/root/.cargo/bin:${PATH}"

COPY native/ ./
RUN maturin build --release --interpreter python3.13

FROM python:3.13-slim-bookworm

WORKDIR /app

COPY docker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=native-builder /build/native/target/wheels/elastic_channels_native-*.whl /tmp/
RUN pip install --no-cache-dir /tmp/elastic_channels_native-*.whl && rm -f /tmp/elastic_channels_native-*.whl

COPY src/elastic_channels/ src/elastic_channels/

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python3"]
CMD ["src/elastic_channels/k8s_worker.py"]
