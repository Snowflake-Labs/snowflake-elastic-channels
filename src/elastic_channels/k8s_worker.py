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

"""Single-worker ingest entrypoint, run one per pod on EKS.

One process per pod (one pod per Kubernetes Indexed Job completion index), no
shared memory across workers. Each pod:

1. Runs one `IngestLoop` against its own elastic channel at a **fixed** target
   rate (`ELASTIC_WORKER_TARGET_BPS`, aggregate target already divided by
   worker count by the caller — see `infra/eks/run_job.sh`).
2. Runs its own SDK Prometheus exporter on a local port (`9090` by default) —
   safe because each pod has its own network namespace.
3. Every `ELASTIC_WORKER_REPORT_INTERVAL_SECONDS` (default 5s), scrapes its own
   `/metrics` endpoint, computes the interval delta, and prints one JSON line
   to stdout. The cluster's monitor (`k8s_monitor.py`) tails these lines via
   the Kubernetes API.
"""

from __future__ import annotations

import json
import os
import re
import signal
import sys
import threading
import time
from typing import Any

from elastic_channels.auth import ingest_client_properties
from elastic_channels.ingest_loop import IngestLoop
from elastic_channels.resort_sales import (
    ResortSalesGenerator,
    estimate_row_bytes,
    native_encoder_available,
)
from elastic_channels.sdk_stats import (
    compute_delta,
    fetch_sdk_metrics,
    maybe_enable_metrics_server,
)

DEFAULT_METRICS_PORT = 9090
DEFAULT_REPORT_INTERVAL_SECONDS = 5.0
CLIENT_CLOSE_TIMEOUT_SECONDS = 30


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    return float(raw) if raw else default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _worker_id() -> int:
    """Resolve this pod's stable worker index.

    Preferred: `JOB_COMPLETION_INDEX`, auto-injected into every container by
    Kubernetes for Indexed Jobs (stable since 1.28). Falls back to parsing the
    trailing `-<N>` ordinal off the pod hostname (also `<job-name>-<index>`
    for Indexed Jobs), then to `ELASTIC_WORKER_ID` for local/manual testing.
    """
    raw = os.environ.get("JOB_COMPLETION_INDEX", "").strip()
    if raw:
        return int(raw)
    hostname = os.environ.get("HOSTNAME", "")
    match = re.search(r"-(\d+)$", hostname)
    if match:
        return int(match.group(1))
    raw = os.environ.get("ELASTIC_WORKER_ID", "").strip()
    if raw:
        return int(raw)
    raise RuntimeError(
        "Could not resolve worker id: set JOB_COMPLETION_INDEX (Indexed Job), "
        "run in a pod with an ordinal hostname, or set ELASTIC_WORKER_ID"
    )


def _emit(record: dict[str, Any]) -> None:
    """Print one JSON line to stdout — the only channel workers report over."""
    record.setdefault("ts", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    print(json.dumps(record, separators=(",", ":")), flush=True)


class _ResettableCounter:
    """Minimal `multiprocessing.Value`-compatible counter for `IngestLoop`'s
    `live_ack_max`. Ack callbacks fire on the SDK's internal thread, so this
    stays lock-protected even though only one process is involved — and
    resettable, so each stdout sample reports this interval's peak batch-ack
    latency rather than the whole run's.
    """

    def __init__(self) -> None:
        self.value = 0
        self._lock = threading.Lock()

    def get_lock(self) -> threading.Lock:
        return self._lock


class _GracefulStop:
    """Translate SIGTERM (Kubernetes pod termination) into a clean loop exit."""

    def __init__(self) -> None:
        self.stop = False
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, _signum: int, _frame: Any) -> None:
        self.stop = True


def main() -> int:
    run_id = os.environ["ELASTIC_RUN_ID"]
    worker_id = _worker_id()
    target_bps = _env_int("ELASTIC_WORKER_TARGET_BPS", 1_000_000)
    duration_seconds = _env_int("ELASTIC_DURATION_SECONDS", 600)
    max_batch_rows = _env_int("ELASTIC_MAX_BATCH_ROWS", 80_000)
    target_batch_bytes = _env_int("ELASTIC_TARGET_BATCH_BYTES", 4_194_304)
    max_in_flight = _env_int("ELASTIC_MAX_IN_FLIGHT", 64)
    seed = _env_int("ELASTIC_SEED", 42)
    # JSON fast-path (see resort_sales.py / fast_append.py): pre-encode NDJSON
    # ourselves (in Rust when the elastic_channels_native wheel is installed,
    # else msgspec) and append it via the elastic channel's FFI directly,
    # skipping the SDK's own dict->JSON re-encode. On by default; falls back
    # automatically to the SDK's default append path when the native wheel
    # isn't installed (falls back to msgspec, still faster than the SDK's own
    # encode; see IngestLoop.run_once and fast_append.supports_fast_append).
    fast_json_encode = _env_bool("ELASTIC_FAST_JSON_ENCODE", True)
    metrics_port = _env_int("ELASTIC_SDK_METRICS_PORT", DEFAULT_METRICS_PORT)
    report_interval = _env_float(
        "ELASTIC_WORKER_REPORT_INTERVAL_SECONDS", DEFAULT_REPORT_INTERVAL_SECONDS
    )
    database = os.environ.get("SNOWFLAKE_DATABASE", "SC_DB")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "SC_SCHEMA")
    pipe_name = os.environ.get("ELASTIC_PIPE_NAME", "RESORT_SALES_PIPE")

    stopper = _GracefulStop()

    maybe_enable_metrics_server(metrics_port)
    from snowflake.ingest.streaming import StreamingIngestClient

    client = None
    channel: Any = None
    exit_code = 0
    try:
        props = ingest_client_properties()
        client = StreamingIngestClient(
            client_name=f"elastic_k8s_{run_id}_w{worker_id}",
            db_name=database,
            schema_name=schema,
            pipe_name=pipe_name,
            properties=props,
        )
        channel = client.get_elastic_channel()

        row_bytes = estimate_row_bytes()
        generator = ResortSalesGenerator(
            seed=seed,
            worker_id=worker_id,
            run_id=run_id,
            use_native=fast_json_encode,
        )
        interval_ack_max = _ResettableCounter()
        loop = IngestLoop(
            channel=channel,
            generator=generator,
            row_bytes=row_bytes,
            max_batch_rows=max_batch_rows,
            target_batch_bytes=target_batch_bytes,
            max_in_flight=max_in_flight,
            floor_rows=min(500, max_batch_rows),
            target_bps_fn=lambda: max(1, target_bps),
            live_ack_max=interval_ack_max,
            use_fast_append=fast_json_encode,
        )

        _emit(
            {
                "type": "start",
                "run_id": run_id,
                "worker_id": worker_id,
                "target_bps": target_bps,
                "duration_seconds": duration_seconds,
                "target_batch_bytes": target_batch_bytes,
                "max_batch_rows": max_batch_rows,
                "row_bytes": row_bytes,
                "workers_total": _env_int("ELASTIC_WORKERS_TOTAL", 0) or None,
                "fast_json_encode": fast_json_encode,
                "native_encoder_available": native_encoder_available(),
            }
        )

        metrics_url = f"http://127.0.0.1:{metrics_port}/metrics"
        prev_scrape: dict[str, float] | None = None
        prev_scrape_time = time.perf_counter()
        prev_rows = 0
        prev_bytes = 0
        prev_ack_sum = 0
        prev_ack_count = 0
        prev_errors = 0
        next_report = time.perf_counter() + report_interval
        deadline = time.perf_counter() + duration_seconds

        while not stopper.stop and time.perf_counter() < deadline:
            _rows, _batch_bytes, sleep_for = loop.run_once()
            if sleep_for > 0:
                time.sleep(min(sleep_for, max(0.0, deadline - time.perf_counter())))

            now = time.perf_counter()
            if now >= next_report:
                interval_s = now - prev_scrape_time
                rows_delta = loop.stats.rows_sent - prev_rows
                bytes_delta = loop.stats.bytes_sent - prev_bytes
                ack_sum_delta = loop.stats.ack_latency_ms_sum - prev_ack_sum
                ack_count_delta = loop.stats.ack_count - prev_ack_count
                errors_delta = loop.stats.errors - prev_errors

                curr_scrape = fetch_sdk_metrics(metrics_url, timeout=1.0)
                sdk_delta = (
                    compute_delta(prev_scrape, curr_scrape, interval_s)
                    if prev_scrape is not None and curr_scrape is not None
                    else None
                )

                _emit(
                    {
                        "type": "sample",
                        "run_id": run_id,
                        "worker_id": worker_id,
                        "interval_s": round(interval_s, 3),
                        "rows_per_sec": round(rows_delta / interval_s, 2)
                        if interval_s > 0
                        else 0.0,
                        "client_bytes_per_sec": round(bytes_delta / interval_s, 2)
                        if interval_s > 0
                        else 0.0,
                        "uncompressed_bps": round(sdk_delta.uncompressed_bps, 2)
                        if sdk_delta
                        else None,
                        "compressed_bps": round(sdk_delta.compressed_bps, 2)
                        if sdk_delta and sdk_delta.compression_metrics_available
                        else None,
                        "compression_ratio": round(sdk_delta.compression_ratio, 2)
                        if sdk_delta and sdk_delta.compression_metrics_available
                        else None,
                        # Cumulative SDK-side count of 429/503 responses that the SDK
                        # retried internally (`sf_api_retryable_error_count`) — a
                        # throttling signal even when every retry eventually
                        # succeeded (so it never shows up in errors_total below).
                        "retryable_errors_total": sdk_delta.retryable_errors_total
                        if sdk_delta
                        else None,
                        "avg_ack_ms": round(ack_sum_delta / ack_count_delta, 1)
                        if ack_count_delta
                        else None,
                        "max_ack_ms": interval_ack_max.value,
                        "batches": ack_count_delta,
                        "errors_interval": errors_delta,
                        "errors_total": loop.stats.errors,
                    }
                )

                if curr_scrape is not None:
                    prev_scrape = curr_scrape
                prev_scrape_time = now
                prev_rows = loop.stats.rows_sent
                prev_bytes = loop.stats.bytes_sent
                prev_ack_sum = loop.stats.ack_latency_ms_sum
                prev_ack_count = loop.stats.ack_count
                prev_errors = loop.stats.errors
                with interval_ack_max.get_lock():
                    interval_ack_max.value = 0
                next_report = now + report_interval

        loop.flush()
        _emit(
            {
                "type": "summary",
                "run_id": run_id,
                "worker_id": worker_id,
                "rows_sent": loop.stats.rows_sent,
                "bytes_sent": loop.stats.bytes_sent,
                "batches_sent": loop.stats.batches_sent,
                "errors": loop.stats.errors,
                "avg_ack_ms": round(
                    loop.stats.ack_latency_ms_sum / loop.stats.ack_count, 1
                )
                if loop.stats.ack_count
                else None,
                "max_ack_ms": loop.stats.ack_latency_max_ms,
                "stopped_early": stopper.stop,
            }
        )
        # Individual batch errors are already counted in the summary above and
        # observable via the monitor/Snowflake table; only a fatal exception
        # (below) marks the pod itself failed to Kubernetes.
    except Exception as exc:  # noqa: BLE001 — surface via stdout for the monitor too
        import traceback

        traceback.print_exc(file=sys.stderr)
        _emit(
            {
                "type": "error",
                "run_id": run_id,
                "worker_id": worker_id,
                "message": repr(exc),
            }
        )
        exit_code = 1
    finally:
        if client is not None:
            try:
                client.close(
                    wait_for_flush=True, timeout_seconds=CLIENT_CLOSE_TIMEOUT_SECONDS
                )
            except Exception:
                pass

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
