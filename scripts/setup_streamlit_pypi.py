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

"""Create PyPI external access integration for Streamlit SPCS deploy."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--app-role",
        default=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )
    args = parser.parse_args()

    import snowflake.connector

    from elastic_channels.auth import connector_config

    sql_path = ROOT / "sql" / "setup" / "setup_streamlit_pypi.sql"
    sql = sql_path.read_text(encoding="utf-8").replace("{app_role}", args.app_role)

    cfg = connector_config()
    warehouse = os.environ.get("SNOWFLAKE_ADMIN_WAREHOUSE") or os.environ.get(
        "SNOWFLAKE_WAREHOUSE", "XSMALL_WH"
    )

    with snowflake.connector.connect(**cfg) as conn:
        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {warehouse}")
        for stmt in [part.strip() for part in sql.split(";") if part.strip()]:
            cur.execute(stmt)
            print(f"OK: {stmt.splitlines()[0][:80]}...")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
