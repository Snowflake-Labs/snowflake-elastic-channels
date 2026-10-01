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

"""Portable Snowflake network-rule / network-policy helpers for setup_elastic_ramp.py.

This project owns two objects:

- schema-level network rule ``{database}.{schema}.ELASTIC_RAMP_INGRESS``
- account-level network policy ``ELASTIC_RAMP_POLICY``

It never mutates lab leftovers (``USER_IPS``, ``limited_policy``). Account-level
policy attach is opt-in and requires an explicit acknowledgement flag because
``ALTER ACCOUNT SET NETWORK_POLICY`` *replaces* the account policy.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

AttachMode = Literal["none", "account", "user"]

PROJECT_POLICY = "ELASTIC_RAMP_POLICY"
PROJECT_RULE = "ELASTIC_RAMP_INGRESS"

_UNQUOTED_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def normalize_ipv4(host: str) -> str:
    """Return bare IPv4 address (no CIDR suffix)."""
    return host.strip().split("/")[0]


def normalize_cidr(entry: str) -> str:
    """Return IPv4 CIDR with /32 suffix (Snowflake network rules require consistent format)."""
    host = normalize_ipv4(entry)
    return f"{host}/32"


def merge_cidrs(existing: list[str], additions: list[str]) -> list[str]:
    """Merge CIDR lists, normalizing to /32 and deduplicating by host."""
    merged: dict[str, str] = {}
    for entry in existing + additions:
        cidr = normalize_cidr(entry)
        merged[normalize_ipv4(cidr)] = cidr
    return list(merged.values())


def value_list_sql(cidrs: list[str]) -> str:
    return ", ".join(f"'{c}'" for c in cidrs)


def quote_ident(name: str) -> str:
    """Render a Snowflake identifier. Already-quoted names are left as-is."""
    stripped = name.strip()
    if len(stripped) >= 2 and stripped.startswith('"') and stripped.endswith('"'):
        return stripped
    if _UNQUOTED_IDENT.fullmatch(stripped):
        return stripped
    return '"' + stripped.replace('"', '""') + '"'


def rule_fqn(database: str, schema: str, rule: str = PROJECT_RULE) -> str:
    return f"{database}.{schema}.{rule}"


def parse_attach_mode(value: str) -> AttachMode:
    normalized = value.strip().lower()
    if normalized not in ("none", "account", "user"):
        raise ValueError(
            f"Invalid --attach-network-policy={value!r}; "
            "expected none, account, or user"
        )
    return normalized  # type: ignore[return-value]


@dataclass(frozen=True)
class NetworkSetupPlan:
    rule_fqn: str
    policy_name: str
    cidrs: tuple[str, ...]
    attach: AttachMode
    existing_policy: str | None
    ingest_user: str | None
    replace_account_ack: bool


def validate_network_attach(
    *,
    attach: AttachMode,
    existing_policy: str | None,
    replace_account_ack: bool,
    ingest_user: str | None,
) -> None:
    existing = (existing_policy or "").strip() or None
    if attach == "account" and existing:
        raise ValueError(
            "Use either --attach-network-policy=account or --existing-network-policy, not both"
        )
    if attach == "user" and existing:
        raise ValueError(
            "Use either --attach-network-policy=user or --existing-network-policy, not both"
        )
    if attach == "account" and not replace_account_ack:
        raise ValueError(
            "Attaching ELASTIC_RAMP_POLICY at the account level replaces the account "
            "network policy and can lock users out. Re-run with "
            "--attach-network-policy=account "
            "--i-understand-this-replaces-the-account-network-policy "
            "after merging ingest and operator IPs into ELASTIC_RAMP_INGRESS "
            "(prefer --attach-network-policy=user)."
        )
    if attach == "user" and not (ingest_user or "").strip():
        raise ValueError(
            "--attach-network-policy=user requires SNOWFLAKE_USER (the ingest user to attach)"
        )
    if replace_account_ack and attach != "account":
        raise ValueError(
            "--i-understand-this-replaces-the-account-network-policy is only valid "
            "with --attach-network-policy=account"
        )


def describe_rule_cidrs(raw_value_list: str | None) -> list[str]:
    if not raw_value_list:
        return []
    return [normalize_cidr(part) for part in str(raw_value_list).split(",") if part.strip()]


def network_sql_statements(plan: NetworkSetupPlan) -> list[str]:
    """SQL to apply after CREATE NETWORK RULE / POLICY IF NOT EXISTS.

    Order: update the project rule CIDRs, keep the project policy pointed at
    that rule, optionally add the rule to a caller-named policy, optionally
    attach the project policy to a user or the account.
    """
    if not plan.cidrs:
        raise ValueError("Network rule VALUE_LIST cannot be empty")
    statements = [
        f"ALTER NETWORK RULE {plan.rule_fqn} SET VALUE_LIST = ({value_list_sql(list(plan.cidrs))})",
        (
            f"ALTER NETWORK POLICY {quote_ident(plan.policy_name)} "
            f"SET ALLOWED_NETWORK_RULE_LIST = ('{plan.rule_fqn}')"
        ),
    ]
    existing = (plan.existing_policy or "").strip() or None
    if existing:
        statements.append(
            f"ALTER NETWORK POLICY {quote_ident(existing)} "
            f"ADD ALLOWED_NETWORK_RULE_LIST = ('{plan.rule_fqn}')"
        )
    if plan.attach == "user":
        user = quote_ident(plan.ingest_user or "")
        statements.append(
            f"ALTER USER {user} SET NETWORK_POLICY = {quote_ident(plan.policy_name)}"
        )
    elif plan.attach == "account":
        statements.append(
            f"ALTER ACCOUNT SET NETWORK_POLICY = {quote_ident(plan.policy_name)}"
        )
    return statements


def attach_none_hint(
    *,
    attach: AttachMode,
    existing_policy: str | None,
) -> str | None:
    if attach != "none" or (existing_policy or "").strip():
        return None
    return (
        "ELASTIC_RAMP_POLICY is not attached. Ingest hosts are allowlisted on "
        "ELASTIC_RAMP_INGRESS only. To enforce the policy for the ingest user:\n"
        "  python scripts/setup_elastic_ramp.py ... --attach-network-policy=user\n"
        "To replace the *account* network policy (destructive):\n"
        "  python scripts/setup_elastic_ramp.py ... --attach-network-policy=account "
        "--i-understand-this-replaces-the-account-network-policy\n"
        "To add ELASTIC_RAMP_INGRESS to a policy you already have:\n"
        "  python scripts/setup_elastic_ramp.py ... --existing-network-policy YOUR_POLICY"
    )
