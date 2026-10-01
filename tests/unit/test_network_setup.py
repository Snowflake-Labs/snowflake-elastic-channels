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

"""Unit tests for portable network-policy SQL (no Snowflake required)."""

import pytest

from elastic_channels.network_setup import (
    NetworkSetupPlan,
    attach_none_hint,
    merge_cidrs,
    network_sql_statements,
    parse_attach_mode,
    validate_network_attach,
)


def _plan(**overrides: object) -> NetworkSetupPlan:
    base = dict(
        rule_fqn="SC_DB.SC_SCHEMA.ELASTIC_RAMP_INGRESS",
        policy_name="ELASTIC_RAMP_POLICY",
        cidrs=("203.0.113.10/32", "198.51.100.2/32"),
        attach="none",
        existing_policy=None,
        ingest_user="DEMO_USER",
        replace_account_ack=False,
    )
    base.update(overrides)
    return NetworkSetupPlan(**base)  # type: ignore[arg-type]


def test_default_sql_updates_project_objects_only() -> None:
    stmts = network_sql_statements(_plan())
    joined = "\n".join(stmts)
    assert stmts[0].startswith("ALTER NETWORK RULE SC_DB.SC_SCHEMA.ELASTIC_RAMP_INGRESS SET VALUE_LIST")
    assert "203.0.113.10/32" in stmts[0]
    assert "198.51.100.2/32" in stmts[0]
    assert "ALTER NETWORK POLICY ELASTIC_RAMP_POLICY SET ALLOWED_NETWORK_RULE_LIST" in stmts[1]
    assert "ALTER ACCOUNT SET NETWORK_POLICY" not in joined
    assert "USER_IPS" not in joined
    assert "limited_policy" not in joined
    assert len(stmts) == 2


def test_merge_includes_orchestrator_and_ingest() -> None:
    merged = merge_cidrs(["1.1.1.1/32"], ["2.2.2.2", "1.1.1.1"])
    assert set(merged) == {"1.1.1.1/32", "2.2.2.2/32"}


def test_account_attach_requires_ack() -> None:
    with pytest.raises(ValueError, match="replaces the account"):
        validate_network_attach(
            attach="account",
            existing_policy=None,
            replace_account_ack=False,
            ingest_user="DEMO_USER",
        )


def test_account_attach_sql_only_with_ack() -> None:
    validate_network_attach(
        attach="account",
        existing_policy=None,
        replace_account_ack=True,
        ingest_user="DEMO_USER",
    )
    stmts = network_sql_statements(_plan(attach="account", replace_account_ack=True))
    assert stmts[-1] == "ALTER ACCOUNT SET NETWORK_POLICY = ELASTIC_RAMP_POLICY"


def test_user_attach_sql() -> None:
    stmts = network_sql_statements(_plan(attach="user"))
    assert stmts[-1] == "ALTER USER DEMO_USER SET NETWORK_POLICY = ELASTIC_RAMP_POLICY"
    assert "ALTER ACCOUNT SET NETWORK_POLICY" not in "\n".join(stmts)


def test_existing_policy_adds_rule() -> None:
    stmts = network_sql_statements(_plan(existing_policy="MY_POLICY"))
    assert any(
        stmt.startswith("ALTER NETWORK POLICY MY_POLICY ADD ALLOWED_NETWORK_RULE_LIST")
        for stmt in stmts
    )
    assert "ALTER ACCOUNT SET NETWORK_POLICY" not in "\n".join(stmts)


def test_existing_and_account_attach_conflict() -> None:
    with pytest.raises(ValueError, match="not both"):
        validate_network_attach(
            attach="account",
            existing_policy="MY_POLICY",
            replace_account_ack=True,
            ingest_user="DEMO_USER",
        )


def test_ack_without_account_attach_rejected() -> None:
    with pytest.raises(ValueError, match="only valid"):
        validate_network_attach(
            attach="none",
            existing_policy=None,
            replace_account_ack=True,
            ingest_user="DEMO_USER",
        )


def test_parse_attach_mode() -> None:
    assert parse_attach_mode("NONE") == "none"
    with pytest.raises(ValueError):
        parse_attach_mode("lock-account")


def test_attach_none_hint_present_only_for_default() -> None:
    assert attach_none_hint(attach="none", existing_policy=None)
    assert attach_none_hint(attach="user", existing_policy=None) is None
    assert attach_none_hint(attach="none", existing_policy="MINE") is None
