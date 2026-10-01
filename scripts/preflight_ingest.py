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

"""Verify Snowflake streaming ingest is reachable before starting the ramp test."""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    args = parser.parse_args()

    from snowflake.ingest.streaming import StreamingIngestClient

    from elastic_channels.auth import ingest_client_properties

    props = ingest_client_properties()
    db = os.environ.get("SNOWFLAKE_DATABASE", "SC_DB")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "SC_SCHEMA")
    pipe = os.environ.get("ELASTIC_PIPE_NAME", "RESORT_SALES_PIPE")

    deadline = time.monotonic() + args.timeout_seconds
    attempt = 0
    last_error = ""

    while time.monotonic() < deadline:
        attempt += 1
        try:
            client = StreamingIngestClient(
                client_name=f"preflight_{attempt}",
                db_name=db,
                schema_name=schema,
                pipe_name=pipe,
                properties=props,
            )
            client.get_elastic_channel()
            client.close(wait_for_flush=False)
            print(f"Preflight ingest OK after {attempt} attempt(s)")
            return 0
        except Exception as exc:  # noqa: BLE001 — surface SDK errors to operator
            last_error = str(exc)
            print(f"Preflight attempt {attempt} failed: {last_error}", file=sys.stderr)
            time.sleep(args.interval_seconds)

    print(f"Preflight ingest failed after {args.timeout_seconds}s: {last_error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
