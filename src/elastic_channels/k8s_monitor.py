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

"""Cluster-resident monitor for the EKS execution path.

Runs as a single, long-lived Deployment pod (created once, independent of any
particular run — see `infra/eks/manifests/monitor-deployment.yaml`). It:

1. Lists worker pods in its namespace by label (`app=elastic-ramp-worker`,
   default from `ELASTIC_WORKER_LABEL_SELECTOR`) via the Kubernetes API
   (in-cluster `ServiceAccount` with RBAC to `get`/`list` pods and
   `get` pods/log — see `infra/eks/manifests/rbac.yaml`).
2. Polls each pod's stdout log (structured one-JSON-object-per-line, written by
   `k8s_worker.py`) every `ELASTIC_MONITOR_POLL_SECONDS`, tracking a per-pod
   high-water mark (the kubelet's own log timestamp, from `timestamps=True`) so
   restarts/re-reads don't double count. The network-bound
   `read_namespaced_pod_log` calls for one poll cycle run concurrently across a
   thread pool (`ELASTIC_MONITOR_LOG_FETCH_CONCURRENCY`, default 48) so a poll
   cycle across hundreds of worker pods completes in seconds rather than
   minutes. All shared state (`_pod_last_ts`, `_runs`) is only ever mutated
   back on the main thread after the pool's I/O completes, so no locking is
   needed.
3. Groups pods by the `run-id` pod label (set on the worker Job template) and,
   every `ELASTIC_MONITOR_FLUSH_SECONDS`, aggregates each run's currently
   reporting workers into one row: summed throughput (rows/sec, uncompressed
   and compressed bytes/sec), batch-ack latency, and error counts.
4. On a slower cadence (`ELASTIC_MONITOR_COMMIT_LATENCY_SECONDS`, since this
   runs a real Snowflake query), also measures true end-to-end commit latency
   (`SALE_TIME` -> `METADATA$ROW_LAST_COMMIT_TIME`) for the run's recent
   window and merges it into that flush's row.
5. Writes each run's aggregated row directly to a Snowflake table
   (`ELASTIC_K8S_METRICS`, see `sql/setup/setup_elastic_k8s_metrics.sql`) —
   there is no local `results/<run-id>/` directory inside the cluster; this
   table *is* the durable record for the EKS path while a run is in flight.

This process never terminates on its own; it is meant to run for the life of
the (persistent) cluster, continuously picking up whichever runs' worker pods
come and go.
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

DEFAULT_LABEL_SELECTOR = "app=elastic-ramp-worker"
DEFAULT_POLL_SECONDS = 10.0
DEFAULT_FLUSH_SECONDS = 30.0
DEFAULT_COMMIT_LATENCY_SECONDS = 120.0
DEFAULT_COMMIT_LATENCY_LAG_SECONDS = 30.0
DEFAULT_TABLE = "ELASTIC_K8S_METRICS"
DEFAULT_LOG_FETCH_CONCURRENCY = 48
# (connect, read) timeout for every Kubernetes API call. Without this, a single
# slow/wedged kubelet or apiserver connection blocks its request forever —
# with `_preload_content=False` there's no client-side default — which hangs
# the whole poll cycle (all pods, not just the slow one) since `_poll_once`
# waits for every fetch before applying any of them.
K8S_REQUEST_TIMEOUT = (5, 20)
# Belt-and-suspenders: some Kubernetes client calls can still block past their
# configured read timeout under load, regardless of `K8S_REQUEST_TIMEOUT` and
# the bounded `concurrent.futures.wait()` in `_poll_once`. Rather than chase
# every possible client wedge, the watchdog thread below force-exits the whole
# process if a poll cycle doesn't complete in time, so the Deployment's
# `restartPolicy: Always` recovers a fresh process instead of silently
# stalling metrics forever.
DEFAULT_WATCHDOG_SECONDS = 90.0
POD_LOG_TAIL_LINES = 2000


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    return float(raw) if raw else default


def _log(msg: str) -> None:
    print(f"[monitor] {msg}", flush=True)


@dataclass
class WorkerSample:
    worker_id: int
    rows_per_sec: float = 0.0
    uncompressed_bps: float | None = None
    compressed_bps: float | None = None
    errors_total: int = 0
    retryable_errors_total: int = 0
    seen_at: float = 0.0


@dataclass
class RunWindow:
    """Accumulates data for one run_id between Snowflake flushes."""

    workers_expected: int = 0
    latest_sample: dict[int, WorkerSample] = field(default_factory=dict)
    batches_since_flush: int = 0
    ack_ms_weighted_sum: float = 0.0
    ack_count_since_flush: int = 0
    ack_max_ms: float = 0.0
    errors_at_last_flush: int = 0
    last_commit_query_end: datetime | None = None


class K8sMonitor:
    def __init__(self) -> None:
        self.namespace = os.environ.get("ELASTIC_NAMESPACE", "elastic-ramp")
        self.label_selector = os.environ.get(
            "ELASTIC_WORKER_LABEL_SELECTOR", DEFAULT_LABEL_SELECTOR
        )
        self.poll_seconds = _env_float("ELASTIC_MONITOR_POLL_SECONDS", DEFAULT_POLL_SECONDS)
        self.log_fetch_concurrency = int(
            os.environ.get("ELASTIC_MONITOR_LOG_FETCH_CONCURRENCY", "")
            or DEFAULT_LOG_FETCH_CONCURRENCY
        )
        self.watchdog_seconds = _env_float(
            "ELASTIC_MONITOR_WATCHDOG_SECONDS", DEFAULT_WATCHDOG_SECONDS
        )
        self.flush_seconds = _env_float(
            "ELASTIC_MONITOR_FLUSH_SECONDS", DEFAULT_FLUSH_SECONDS
        )
        self.commit_latency_seconds = _env_float(
            "ELASTIC_MONITOR_COMMIT_LATENCY_SECONDS", DEFAULT_COMMIT_LATENCY_SECONDS
        )
        self.commit_latency_lag_seconds = _env_float(
            "ELASTIC_MONITOR_COMMIT_LATENCY_LAG_SECONDS",
            DEFAULT_COMMIT_LATENCY_LAG_SECONDS,
        )
        self.commit_latency_enabled = (
            os.environ.get("ELASTIC_MONITOR_COMMIT_LATENCY_ENABLED", "true")
            .strip()
            .lower()
            not in {"0", "false", "no", "off"}
        )
        self.database = os.environ.get("SNOWFLAKE_DATABASE", "SC_DB")
        self.schema = os.environ.get("SNOWFLAKE_SCHEMA", "SC_SCHEMA")
        self.metrics_table = os.environ.get("ELASTIC_K8S_METRICS_TABLE", DEFAULT_TABLE)
        self.sales_table_name = os.environ.get("ELASTIC_TABLE_NAME", "RESORT_SALES")
        self.sales_table = f"{self.database}.{self.schema}.{self.sales_table_name}"
        self.commit_latency_warehouse = os.environ.get(
            "ELASTIC_MONITOR_WAREHOUSE"
        ) or os.environ.get("SNOWFLAKE_WAREHOUSE", "XSMALL_WH")

        self._core_v1_client = None
        self._log_pool_executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._sf_conn = None
        self._last_poll_at = time.monotonic()
        # Overridable in tests so we can assert the watchdog *would* have
        # fired without actually killing the test process.
        self._watchdog_exit = os._exit

        self._pod_last_ts: dict[str, str] = {}
        self._pod_run_id: dict[str, str] = {}
        self._runs: dict[str, RunWindow] = defaultdict(RunWindow)

    @property
    def _core_v1(self):
        """Lazily built so unit tests can exercise parsing/aggregation logic
        without a live cluster or kubeconfig."""
        if self._core_v1_client is None:
            from kubernetes import client, config

            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()
            # Default urllib3 pool (maxsize=4-ish) serializes concurrent
            # requests from the thread pool in `_poll_once` right back into
            # queued connections — raise it to match so log-fetch concurrency
            # is actually concurrent on the wire, not just in Python threads.
            configuration = client.Configuration.get_default_copy()
            configuration.connection_pool_maxsize = max(
                self.log_fetch_concurrency, configuration.connection_pool_maxsize or 0
            )
            self._core_v1_client = client.CoreV1Api(client.ApiClient(configuration))
        return self._core_v1_client

    def _snowflake_conn(self):
        if self._sf_conn is not None:
            try:
                self._sf_conn.cursor().execute("SELECT 1")
                return self._sf_conn
            except Exception:
                try:
                    self._sf_conn.close()
                except Exception:
                    pass
                self._sf_conn = None

        import snowflake.connector

        from elastic_channels.auth import connector_config

        cfg = connector_config()
        self._sf_conn = snowflake.connector.connect(**cfg)
        cur = self._sf_conn.cursor()
        cur.execute(f"USE WAREHOUSE {self.commit_latency_warehouse}")
        _log(f"connected to Snowflake account={cfg.get('account')}")
        return self._sf_conn

    # ---- pod discovery + log tailing -------------------------------------

    def _list_worker_pods(self) -> list[Any]:
        try:
            resp = self._core_v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=self.label_selector,
                _request_timeout=K8S_REQUEST_TIMEOUT,
            )
        except Exception as exc:  # noqa: BLE001
            _log(f"list pods failed: {exc!r}")
            return []
        return resp.items

    def _fetch_pod_log(self, pod_name: str) -> str | None:
        """Pure I/O — the only part of log-tailing that runs off the main thread
        (see `_poll_once`). Returns the raw decoded log text, or None on error."""
        try:
            # `_preload_content=False` + manual decode avoids a kubernetes-client
            # deserialization issue where `read_namespaced_pod_log` can return the
            # *string repr* of the raw bytes response (e.g. "b'2026-...'", literal
            # backslash-n's and all) instead of a decoded str, silently breaking
            # `.splitlines()`.
            resp = self._core_v1.read_namespaced_pod_log(
                name=pod_name,
                namespace=self.namespace,
                timestamps=True,
                since_seconds=int(self.poll_seconds * 3) + 15,
                tail_lines=POD_LOG_TAIL_LINES,
                _preload_content=False,
                _request_timeout=K8S_REQUEST_TIMEOUT,
            )
            return resp.data.decode("utf-8", errors="replace")
        except Exception as exc:  # noqa: BLE001 — pod may be gone/starting/no logs yet
            _log(f"read log failed for {pod_name}: {exc!r}")
            return None

    @staticmethod
    def _parse_new_lines(raw: str, last_seen: str) -> tuple[list[str], str]:
        """Pure parsing — split a pod's raw kubelet-timestamped log into
        not-yet-seen JSON payloads, and the new high-water-mark timestamp."""
        new_lines: list[str] = []
        newest = last_seen
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split(" ", 1)
            if len(parts) != 2:
                continue
            ts, payload = parts
            if ts <= last_seen:
                continue
            new_lines.append(payload)
            newest = max(newest, ts)
        return new_lines, newest

    def _read_new_lines(self, pod_name: str) -> list[str]:
        """Single-pod fetch + parse + advance high-water-mark. Used directly by
        unit tests (and available for manual/debugging use); `_poll_once` uses
        the thread-pooled fetch below instead so it scales to hundreds of pods."""
        raw = self._fetch_pod_log(pod_name)
        if raw is None:
            return []
        last_seen = self._pod_last_ts.get(pod_name, "")
        new_lines, newest = self._parse_new_lines(raw, last_seen)
        self._pod_last_ts[pod_name] = newest
        return new_lines

    @property
    def _log_pool(self) -> concurrent.futures.ThreadPoolExecutor:
        """One long-lived pool for the monitor's whole lifetime, not one per
        poll. `_poll_once` bounds how long it *waits* on futures (see below)
        rather than relying on `ThreadPoolExecutor.shutdown()`'s implicit
        `wait=True` join — a single wedged `read_namespaced_pod_log` call could
        otherwise freeze every future poll cycle, not just the slow one. A
        persistent pool means an occasional wedge just permanently parks one
        worker thread (harmless up to `log_fetch_concurrency` occurrences)
        instead of recreating — and re-blocking forever on — a fresh pool
        every cycle."""
        if self._log_pool_executor is None:
            self._log_pool_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.log_fetch_concurrency
            )
        return self._log_pool_executor

    def _poll_once(self) -> None:
        pods = self._list_worker_pods()
        if not pods:
            return

        pod_names = [pod.metadata.name for pod in pods]
        pool = self._log_pool
        futures = {pool.submit(self._fetch_pod_log, name): name for name in pod_names}
        # Bound the wait regardless of individual request timeouts actually
        # firing (see `_log_pool` docstring) — any future still pending past
        # this deadline is treated as failed for this cycle; its thread stays
        # parked in the pool and simply won't be picked for new work again if
        # it never returns.
        deadline = K8S_REQUEST_TIMEOUT[1] + 15
        done, not_done = concurrent.futures.wait(futures, timeout=deadline)
        raw_by_pod: dict[str, str | None] = {}
        for fut in done:
            pod_name = futures[fut]
            try:
                raw_by_pod[pod_name] = fut.result()
            except Exception as exc:  # noqa: BLE001
                _log(f"read log failed for {pod_name}: {exc!r}")
                raw_by_pod[pod_name] = None
        for fut in not_done:
            pod_name = futures[fut]
            _log(f"read log timed out after {deadline}s (pool-bounded) for {pod_name}")
            raw_by_pod[pod_name] = None

        # Apply results back on the main thread only — no lock needed since
        # nothing above touches _pod_last_ts/_pod_run_id/_runs.
        for pod in pods:
            pod_name = pod.metadata.name
            raw = raw_by_pod.get(pod_name)
            run_id = (pod.metadata.labels or {}).get("run-id", "unknown")
            self._pod_run_id[pod_name] = run_id
            if raw is None:
                continue
            run = self._runs[run_id]
            last_seen = self._pod_last_ts.get(pod_name, "")
            new_lines, newest = self._parse_new_lines(raw, last_seen)
            self._pod_last_ts[pod_name] = newest
            for payload in new_lines:
                try:
                    record = json.loads(payload)
                except (json.JSONDecodeError, ValueError):
                    continue
                self._apply_record(run, record)

    def _apply_record(self, run: RunWindow, record: dict[str, Any]) -> None:
        rtype = record.get("type")
        worker_id = record.get("worker_id")
        if worker_id is None:
            return

        if rtype == "sample":
            sample = WorkerSample(
                worker_id=worker_id,
                rows_per_sec=float(record.get("rows_per_sec") or 0.0),
                uncompressed_bps=record.get("uncompressed_bps"),
                compressed_bps=record.get("compressed_bps"),
                errors_total=int(record.get("errors_total") or 0),
                retryable_errors_total=int(record.get("retryable_errors_total") or 0),
                seen_at=time.time(),
            )
            run.latest_sample[worker_id] = sample
            run.batches_since_flush += int(record.get("batches") or 0)
            avg_ack = record.get("avg_ack_ms")
            batches = int(record.get("batches") or 0)
            if avg_ack is not None and batches:
                run.ack_ms_weighted_sum += float(avg_ack) * batches
                run.ack_count_since_flush += batches
            max_ack = record.get("max_ack_ms")
            if max_ack is not None:
                run.ack_max_ms = max(run.ack_max_ms, float(max_ack))
        elif rtype == "start":
            workers_total = record.get("workers_total")
            if workers_total:
                run.workers_expected = int(workers_total)
        elif rtype in ("summary", "error"):
            errors_total = record.get("errors_total") or record.get("errors")
            if errors_total is not None and worker_id in run.latest_sample:
                run.latest_sample[worker_id].errors_total = int(errors_total)

    # ---- aggregation + Snowflake write ------------------------------------

    def _flush_run(self, run_id: str, run: RunWindow, window_start: datetime, window_end: datetime) -> None:
        cutoff = time.time() - (self.poll_seconds * 4 + 30)
        fresh = {
            wid: s for wid, s in run.latest_sample.items() if s.seen_at >= cutoff
        }
        if not fresh and run.batches_since_flush == 0:
            return  # nothing new for this run this window; skip empty row

        rows_per_sec = sum(s.rows_per_sec for s in fresh.values())
        uncompressed_bps = sum(
            s.uncompressed_bps for s in fresh.values() if s.uncompressed_bps is not None
        )
        comp_samples = [s.compressed_bps for s in fresh.values() if s.compressed_bps is not None]
        compressed_bps = sum(comp_samples) if comp_samples else None
        compression_ratio = (
            uncompressed_bps / compressed_bps
            if compressed_bps and compressed_bps > 0
            else None
        )
        errors_total = sum(s.errors_total for s in run.latest_sample.values())
        retryable_errors_total = sum(
            s.retryable_errors_total for s in run.latest_sample.values()
        )
        avg_ack_ms = (
            run.ack_ms_weighted_sum / run.ack_count_since_flush
            if run.ack_count_since_flush
            else None
        )

        commit_avg_ms = commit_max_ms = commit_sampled = None
        if self.commit_latency_enabled:
            commit_avg_ms, commit_max_ms, commit_sampled = self._maybe_commit_latency(
                run_id, run, window_end
            )

        row = {
            "RUN_ID": run_id,
            "WINDOW_START": window_start,
            "WINDOW_END": window_end,
            "WORKERS_REPORTING": len(fresh),
            "WORKERS_EXPECTED": run.workers_expected or None,
            "ROWS_PER_SEC": rows_per_sec,
            "UNCOMPRESSED_BYTES_PER_SEC": uncompressed_bps,
            "COMPRESSED_BYTES_PER_SEC": compressed_bps,
            "COMPRESSION_RATIO": compression_ratio,
            "AVG_BATCH_ACK_MS": avg_ack_ms,
            "MAX_BATCH_ACK_MS": run.ack_max_ms or None,
            "BATCHES_ACKED": run.batches_since_flush,
            "ERRORS_TOTAL": errors_total,
            "RETRYABLE_ERRORS_TOTAL": retryable_errors_total,
            "AVG_COMMIT_LATENCY_MS": commit_avg_ms,
            "MAX_COMMIT_LATENCY_MS": commit_max_ms,
            "COMMIT_LATENCY_ROWS_SAMPLED": commit_sampled,
        }
        self._write_row(row)

        run.batches_since_flush = 0
        run.ack_ms_weighted_sum = 0.0
        run.ack_count_since_flush = 0
        run.ack_max_ms = 0.0

    def _maybe_commit_latency(
        self, run_id: str, run: RunWindow, now: datetime
    ) -> tuple[float | None, float | None, int | None]:
        lag = timedelta(seconds=self.commit_latency_lag_seconds)
        window_end = now - lag
        if run.last_commit_query_end is not None:
            elapsed = (window_end - run.last_commit_query_end).total_seconds()
            if elapsed < self.commit_latency_seconds:
                return None, None, None
            window_start = run.last_commit_query_end
        else:
            window_start = window_end - timedelta(seconds=self.commit_latency_seconds)

        if window_end <= window_start:
            return None, None, None

        try:
            conn = self._snowflake_conn()
            cur = conn.cursor()
            sql = f"""
                SELECT
                    COUNT(*),
                    AVG(TIMESTAMPDIFF('millisecond',
                        TO_TIMESTAMP_TZ(SALE_TIME || ' +0000'), METADATA$ROW_LAST_COMMIT_TIME)),
                    MAX(TIMESTAMPDIFF('millisecond',
                        TO_TIMESTAMP_TZ(SALE_TIME || ' +0000'), METADATA$ROW_LAST_COMMIT_TIME))
                FROM {self.sales_table}
                WHERE RUN_ID = %s
                  AND SALE_TIME >= TO_TIMESTAMP_NTZ(%s)
                  AND SALE_TIME < TO_TIMESTAMP_NTZ(%s)
                  AND METADATA$ROW_LAST_COMMIT_TIME IS NOT NULL
            """
            start_ntz = window_start.strftime("%Y-%m-%d %H:%M:%S")
            end_ntz = window_end.strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(sql, (run_id, start_ntz, end_ntz))
            count, avg_ms, max_ms = cur.fetchone()
            run.last_commit_query_end = window_end
            if not count:
                return None, None, 0
            return float(avg_ms or 0.0), float(max_ms or 0.0), int(count)
        except Exception as exc:  # noqa: BLE001 — commit latency is best-effort
            _log(f"commit latency query failed for {run_id}: {exc!r}")
            return None, None, None

    def _write_row(self, row: dict[str, Any]) -> None:
        cols = list(row.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        sql = (
            f"INSERT INTO {self.database}.{self.schema}.{self.metrics_table} "
            f"({', '.join(cols)}) VALUES ({placeholders})"
        )
        values = [row[c] for c in cols]
        try:
            conn = self._snowflake_conn()
            conn.cursor().execute(sql, values)
            _log(
                f"run={row['RUN_ID']} rows_per_sec={row['ROWS_PER_SEC']:.0f} "
                f"uncompressed_bps={row['UNCOMPRESSED_BYTES_PER_SEC']:.0f} "
                f"compressed_bps={row['COMPRESSED_BYTES_PER_SEC']} "
                f"workers={row['WORKERS_REPORTING']} errors={row['ERRORS_TOTAL']} "
                f"retryable_errors={row['RETRYABLE_ERRORS_TOTAL']}"
            )
        except Exception as exc:  # noqa: BLE001
            _log(f"Snowflake insert failed: {exc!r}")

    # ---- main loop ---------------------------------------------------------

    def _watchdog_check_once(self) -> bool:
        """One stall check — split out from `_watchdog_loop` so tests can drive
        it directly instead of depending on real sleep timing. Returns True
        (and has already called `_watchdog_exit`) if it fired."""
        stalled_for = time.monotonic() - self._last_poll_at
        if stalled_for > self.watchdog_seconds:
            _log(
                f"WATCHDOG: no completed poll cycle in {stalled_for:.0f}s "
                f"(limit {self.watchdog_seconds:.0f}s) — force-exiting so the "
                "Deployment restarts a fresh process."
            )
            sys.stdout.flush()
            self._watchdog_exit(1)
            return True
        return False

    def _watchdog_loop(self) -> None:
        """Daemon thread for the process lifetime — see `DEFAULT_WATCHDOG_SECONDS`
        for why this exists. Checks more often than the timeout so we don't
        overshoot it by much."""
        check_interval = max(5.0, self.watchdog_seconds / 4)
        while True:
            time.sleep(check_interval)
            if self._watchdog_check_once():
                return

    def run(self) -> None:
        _log(
            f"starting: namespace={self.namespace} selector={self.label_selector} "
            f"poll={self.poll_seconds}s flush={self.flush_seconds}s "
            f"log_fetch_concurrency={self.log_fetch_concurrency} "
            f"watchdog={self.watchdog_seconds}s "
            f"commit_latency_enabled={self.commit_latency_enabled}"
        )
        self._last_poll_at = time.monotonic()
        threading.Thread(target=self._watchdog_loop, daemon=True, name="watchdog").start()
        next_flush = time.monotonic() + self.flush_seconds
        window_start = datetime.now(timezone.utc)
        while True:
            self._poll_once()
            self._last_poll_at = time.monotonic()
            now_mono = time.monotonic()
            if now_mono >= next_flush:
                window_end = datetime.now(timezone.utc)
                for run_id in list(self._runs.keys()):
                    self._flush_run(run_id, self._runs[run_id], window_start, window_end)
                window_start = window_end
                next_flush = now_mono + self.flush_seconds
            time.sleep(self.poll_seconds)


def main() -> int:
    monitor = K8sMonitor()
    try:
        monitor.run()
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
