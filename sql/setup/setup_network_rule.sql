-- Copyright 2026 Snowflake Inc.
-- SPDX-License-Identifier: Apache-2.0
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Network rule allowing ingest from the benchmark's ingress IP(s) (INGRESS to
-- Snowflake service) — populated with the EKS cluster's NAT gateway IP(s) and
-- optionally the operator CLI host; see infra/eks/allowlist_eks.sh, which
-- calls scripts/setup_elastic_ramp.py --ingest-ips / --orchestrator-ip.
-- {ingest_ip} is substituted by scripts/setup_elastic_ramp.py (host only; script adds /32).

CREATE NETWORK RULE IF NOT EXISTS ELASTIC_RAMP_INGRESS
  TYPE = IPV4
  MODE = INGRESS
  VALUE_LIST = ('{ingest_ip}/32')
  COMMENT = 'Elastic channel ramp test ingress IP(s) (EKS NAT gateway + operator host)';

-- Project-owned policy. Setup updates ALLOWED_NETWORK_RULE_LIST to the
-- schema-qualified ELASTIC_RAMP_INGRESS rule. It is not attached to the
-- account unless the operator passes --attach-network-policy=.
CREATE NETWORK POLICY IF NOT EXISTS ELASTIC_RAMP_POLICY
  ALLOWED_NETWORK_RULE_LIST = ('ELASTIC_RAMP_INGRESS')
  COMMENT = 'Allow elastic ramp ingest (and operator) source IPs';
