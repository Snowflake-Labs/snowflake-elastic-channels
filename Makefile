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

.PHONY: test lint eks-up run-benchmark eks-down render-architecture deploy-streamlit help

RUN_ID ?=
TARGET_BPS ?=
DURATION_SECONDS ?=
WORKERS ?= 144

help:
	@echo "  make test                             Run unit tests"
	@echo "  make lint                             Run ruff"
	@echo "  make eks-up                           Create EKS cluster, build+push image, deploy monitor"
	@echo "  make run-benchmark RUN_ID=... TARGET_BPS=... DURATION_SECONDS=... [WORKERS=144]"
	@echo "                                         Run a fixed-throughput benchmark on EKS"
	@echo "  make eks-down                          Scale EKS worker nodes to zero"
	@echo "  make render-architecture              Regenerate docs/architecture.png"
	@echo "  make deploy-streamlit                 Deploy the Streamlit monitor dashboard"

test:
	PYTHONPATH=src pytest tests/unit -q

lint:
	@command -v ruff >/dev/null || { echo "ruff not installed; skip"; exit 0; }
	ruff check src tests scripts streamlit

eks-up:
	./infra/eks/create_cluster.sh
	./infra/eks/build_and_push.sh
	./infra/eks/create_secret.sh
	./infra/eks/allowlist_eks.sh
	./infra/eks/deploy_monitor.sh

run-benchmark:
	@test -n "$(RUN_ID)" || (echo "RUN_ID required" && exit 1)
	@test -n "$(TARGET_BPS)" || (echo "TARGET_BPS required" && exit 1)
	@test -n "$(DURATION_SECONDS)" || (echo "DURATION_SECONDS required" && exit 1)
	./infra/eks/run_job.sh $(RUN_ID) --target-bps $(TARGET_BPS) \
		--duration-seconds $(DURATION_SECONDS) --workers $(WORKERS)

eks-down:
	./infra/eks/teardown_workers.sh

render-architecture:
	./scripts/render_architecture.sh

deploy-streamlit:
	./scripts/deploy_streamlit.sh
