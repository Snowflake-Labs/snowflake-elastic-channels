#!/usr/bin/env python3
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

"""Create Snowflake objects and network rule for the elastic channel ramp test."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from elastic_channels.network_setup import (  # noqa: E402
    PROJECT_POLICY,
    NetworkSetupPlan,
    attach_none_hint,
    merge_cidrs,
    network_sql_statements,
    normalize_ipv4,
    parse_attach_mode,
    rule_fqn as network_rule_fqn,
    validate_network_attach,
)


def _split_sql(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    statements: list[str] = []
    buf: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        buf.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(buf))
            buf = []
    if buf:
        statements.append("\n".join(buf))
    return [s for s in statements if s.strip()]


def _describe_rule_cidrs(cur, rule_fqn: str) -> list[str]:
    from elastic_channels.network_setup import describe_rule_cidrs

    cur.execute(f"DESCRIBE NETWORK RULE {rule_fqn}")
    row = cur.fetchone()
    if not row:
        return []
    return describe_rule_cidrs(str(row[-1]))


_K8S_METRICS_COLUMNS = [
    "RUN_ID",
    "WINDOW_START",
    "WINDOW_END",
    "WORKERS_REPORTING",
    "WORKERS_EXPECTED",
    "ROWS_PER_SEC",
    "UNCOMPRESSED_BYTES_PER_SEC",
    "COMPRESSED_BYTES_PER_SEC",
    "COMPRESSION_RATIO",
    "AVG_BATCH_ACK_MS",
    "MAX_BATCH_ACK_MS",
    "BATCHES_ACKED",
    "ERRORS_TOTAL",
    "RETRYABLE_ERRORS_TOTAL",
    "AVG_COMMIT_LATENCY_MS",
    "MAX_COMMIT_LATENCY_MS",
    "COMMIT_LATENCY_ROWS_SAMPLED",
    "INSERTED_AT",
]


def _migrate_k8s_metrics_to_hybrid(cur, *, database: str, schema: str) -> str | None:
    """Rename a pre-existing *standard*-table ELASTIC_K8S_METRICS out of the way so
    setup_elastic_k8s_metrics.sql's `CREATE HYBRID TABLE IF NOT EXISTS` actually creates
    the hybrid table, instead of being a silent no-op against a same-named object of the
    wrong type (Snowflake can't ALTER a table's type in place, and CREATE ... IF NOT
    EXISTS only checks whether a name is taken, not whether it's the right kind of
    object). Returns the backup table name if a rename happened (the caller must copy
    its rows into the new hybrid table and drop it after running the .sql file), or None
    if there's nothing to migrate (table doesn't exist yet, or is already hybrid).
    """
    fq_name = f"{database}.{schema}.ELASTIC_K8S_METRICS"
    cur.execute(f"SHOW TABLES LIKE 'ELASTIC_K8S_METRICS' IN SCHEMA {database}.{schema}")
    rows = cur.fetchall()
    if not rows:
        return None
    cols = [c[0].lower() for c in cur.description]
    is_hybrid = rows[0][cols.index("is_hybrid")]
    if is_hybrid == "Y":
        return None
    backup_name = f"{fq_name}_PRE_HYBRID"
    cur.execute(f"ALTER TABLE {fq_name} RENAME TO {backup_name}")
    print(
        f"OK: renamed existing standard table {fq_name} -> {backup_name} "
        "(rows will be copied into the new hybrid table below)"
    )
    return backup_name


def _finish_k8s_metrics_migration(
    cur, *, database: str, schema: str, backup_name: str
) -> None:
    """Copy rows from the renamed standard table into the just-created hybrid table
    and drop the backup. Best-effort: a PRIMARY KEY (RUN_ID, WINDOW_START) violation
    from unexpected duplicate/legacy rows must not block the rest of setup — the old
    data stays in `backup_name` for manual inspection/migration if this fails.
    """
    fq_name = f"{database}.{schema}.ELASTIC_K8S_METRICS"
    col_list = ", ".join(_K8S_METRICS_COLUMNS)
    try:
        cur.execute(f"INSERT INTO {fq_name} ({col_list}) SELECT {col_list} FROM {backup_name}")
        print(f"OK: copied {cur.rowcount} row(s) {backup_name} -> {fq_name}")
        cur.execute(f"DROP TABLE {backup_name}")
        print(f"OK: dropped {backup_name}")
    except Exception as exc:  # noqa: BLE001 — best-effort data carry-over, not fatal
        print(
            f"WARNING: could not copy rows from {backup_name} into {fq_name} ({exc!r}); "
            f"old data is preserved in {backup_name} for manual migration",
            file=sys.stderr,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingest-ip", help="Public IPv4 of one ingest host")
    parser.add_argument(
        "--ingest-ips",
        help=(
            "Comma-separated public IPv4 addresses for multi-node/EKS ingest "
            "(e.g. the EKS cluster's NAT gateway IP(s); see infra/eks/allowlist_eks.sh)"
        ),
    )
    parser.add_argument(
        "--orchestrator-ip",
        help=(
            "Public IPv4 of the operator/CLI host; merged into ELASTIC_RAMP_INGRESS "
            "with the ingest IPs so attaching ELASTIC_RAMP_POLICY does not lock you out"
        ),
    )
    parser.add_argument(
        "--attach-network-policy",
        default="none",
        help=(
            "none (default): create/update ELASTIC_RAMP_INGRESS and ELASTIC_RAMP_POLICY only. "
            "user: ALTER USER <SNOWFLAKE_USER> SET NETWORK_POLICY = ELASTIC_RAMP_POLICY. "
            "account: ALTER ACCOUNT SET NETWORK_POLICY (destructive; requires "
            "--i-understand-this-replaces-the-account-network-policy)"
        ),
    )
    parser.add_argument(
        "--i-understand-this-replaces-the-account-network-policy",
        dest="replace_account_ack",
        action="store_true",
        help="Required acknowledgement for --attach-network-policy=account",
    )
    parser.add_argument(
        "--existing-network-policy",
        default=None,
        help=(
            "Optional: ADD ELASTIC_RAMP_INGRESS to a network policy you already have "
            "(name you supply). Mutually exclusive with --attach-network-policy=account|user"
        ),
    )
    parser.add_argument(
        "--apply-network-policy",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--ingest-role",
        default=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )
    parser.add_argument(
        "--bypass-mins",
        type=int,
        default=int(os.environ.get("SNOWFLAKE_BYPASS_NETWORK_POLICY_MINS", "0")),
        help="If >0, temporarily bypass network policy for SNOWFLAKE_USER (ingest window)",
    )
    args = parser.parse_args()

    if args.apply_network_policy:
        print(
            "--apply-network-policy was removed. To attach ELASTIC_RAMP_POLICY at the "
            "account level (replaces the account policy), use:\n"
            "  --attach-network-policy=account "
            "--i-understand-this-replaces-the-account-network-policy\n"
            "Prefer --attach-network-policy=user, or leave the default (none).",
            file=sys.stderr,
        )
        return 2

    try:
        attach = parse_attach_mode(args.attach_network_policy)
        validate_network_attach(
            attach=attach,
            existing_policy=args.existing_network_policy,
            replace_account_ack=args.replace_account_ack,
            ingest_user=os.environ.get("SNOWFLAKE_USER"),
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    ingest_hosts: list[str] = []
    if args.ingest_ips:
        ingest_hosts.extend(
            normalize_ipv4(part) for part in args.ingest_ips.split(",") if part.strip()
        )
    if args.ingest_ip:
        ingest_hosts.append(normalize_ipv4(args.ingest_ip))
    ingest_hosts = list(dict.fromkeys(ingest_hosts))

    extra_hosts: list[str] = []
    if args.orchestrator_ip:
        extra_hosts.append(normalize_ipv4(args.orchestrator_ip))
    extra_hosts = list(dict.fromkeys(extra_hosts))

    if not ingest_hosts:
        print("Provide --ingest-ip or --ingest-ips", file=sys.stderr)
        return 1

    database = os.environ.get("SNOWFLAKE_DATABASE", "SC_DB")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "SC_SCHEMA")
    warehouse = os.environ.get("SNOWFLAKE_ADMIN_WAREHOUSE") or os.environ.get(
        "SNOWFLAKE_WAREHOUSE", "XSMALL"
    )

    import snowflake.connector

    from elastic_channels.auth import connector_config

    cfg = connector_config()
    if not cfg.get("password") and not cfg.get("private_key"):
        print(
            "Set SNOWFLAKE_PRIVATE_KEY(_PATH) or SNOWFLAKE_PASSWORD for setup",
            file=sys.stderr,
        )
        return 1

    connect_kwargs = dict(cfg)
    sql_path = ROOT / "sql" / "setup" / "setup_elastic_ramp.sql"
    net_path = ROOT / "sql" / "setup" / "setup_network_rule.sql"
    k8s_metrics_path = ROOT / "sql" / "setup" / "setup_elastic_k8s_metrics.sql"
    primary_host = ingest_hosts[0]

    def substitute(sql: str) -> str:
        return (
            sql.replace("{database}", database)
            .replace("{schema}", schema)
            .replace("{ingest_role}", args.ingest_role)
            .replace("{ingest_ip}", primary_host)
        )

    addition_cidrs = [f"{host}/32" for host in ingest_hosts] + [
        f"{host}/32" for host in extra_hosts if host not in ingest_hosts
    ]
    rule_fqn = network_rule_fqn(database, schema)

    print(f"Connecting to Snowflake account={cfg.get('account')} role={cfg.get('role')}")
    with snowflake.connector.connect(**connect_kwargs) as conn:
        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {warehouse}")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")

        for stmt in _split_sql(sql_path):
            cur.execute(substitute(stmt))
            print(f"OK: {stmt.splitlines()[0][:80]}...")

        for stmt in _split_sql(net_path):
            cur.execute(substitute(stmt))
            print(f"OK: {stmt.splitlines()[0][:80]}...")

        k8s_metrics_backup = _migrate_k8s_metrics_to_hybrid(
            cur, database=database, schema=schema
        )
        for stmt in _split_sql(k8s_metrics_path):
            cur.execute(substitute(stmt))
            print(f"OK: {stmt.splitlines()[0][:80]}...")
        if k8s_metrics_backup:
            _finish_k8s_metrics_migration(
                cur, database=database, schema=schema, backup_name=k8s_metrics_backup
            )

        resort_sales = f"{database}.{schema}.RESORT_SALES"
        cur.execute(f"TRUNCATE TABLE IF EXISTS {resort_sales}")
        print(f"OK: TRUNCATE TABLE {resort_sales}")
        try:
            cur.execute(f"ALTER TABLE {resort_sales} DROP CLUSTERING KEY")
            print(f"OK: ALTER TABLE {resort_sales} DROP CLUSTERING KEY")
        except snowflake.connector.errors.ProgrammingError as exc:
            if "does not have a clustering key" not in str(exc).lower():
                raise
            print(f"OK: {resort_sales} has no clustering key")

        existing_rule_cidrs: list[str] = []
        try:
            existing_rule_cidrs = _describe_rule_cidrs(cur, rule_fqn)
        except snowflake.connector.errors.ProgrammingError:
            existing_rule_cidrs = []
        planned_cidrs = merge_cidrs(existing_rule_cidrs, addition_cidrs)
        plan = NetworkSetupPlan(
            rule_fqn=rule_fqn,
            policy_name=PROJECT_POLICY,
            cidrs=tuple(planned_cidrs),
            attach=attach,
            existing_policy=args.existing_network_policy,
            ingest_user=os.environ.get("SNOWFLAKE_USER"),
            replace_account_ack=args.replace_account_ack,
        )
        for stmt in network_sql_statements(plan):
            try:
                cur.execute(stmt)
                print(f"OK: {stmt}")
            except snowflake.connector.errors.ProgrammingError as exc:
                if "already exists" in str(exc).lower() and "ADD ALLOWED_NETWORK_RULE_LIST" in stmt:
                    print(f"OK: rule already on policy ({exc})")
                else:
                    raise
        hint = attach_none_hint(attach=attach, existing_policy=args.existing_network_policy)
        if hint:
            print(hint)

        if args.bypass_mins > 0:
            ingest_user = os.environ["SNOWFLAKE_USER"]
            try:
                cur.execute(
                    f"ALTER USER {ingest_user} SET MINS_TO_BYPASS_NETWORK_POLICY = {args.bypass_mins}"
                )
                print(
                    f"OK: ALTER USER {ingest_user} SET MINS_TO_BYPASS_NETWORK_POLICY = {args.bypass_mins}"
                )
            except snowflake.connector.errors.ProgrammingError as exc:
                print(
                    f"WARN: could not set MINS_TO_BYPASS_NETWORK_POLICY on {ingest_user}: {exc}",
                    file=sys.stderr,
                )

        cur.execute(f"SELECT COUNT(*) FROM {database}.{schema}.RESORT_SALES")
        count = cur.fetchone()[0]
        print(f"Setup complete. RESORT_SALES row count={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
