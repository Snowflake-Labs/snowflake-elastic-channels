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

"""Shared elastic-channel ingest batch sizing and rate-limited append loop."""

from __future__ import annotations

import concurrent.futures
import sys
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from elastic_channels import fast_append
from elastic_channels.resort_sales import ResortSalesGenerator


def target_batch_rows(
    worker_bps: int,
    row_bytes: int,
    *,
    max_rows: int,
    target_batch_bytes: int,
    floor_rows: int,
) -> int:
    """Size batches for SDK throughput (~1 MiB) while honoring the rate target."""
    rows_for_bytes = max(1, target_batch_bytes // row_bytes)
    rows_for_rate = max(1, worker_bps // max(1, row_bytes * 4))
    return max(floor_rows, min(max_rows, rows_for_bytes, rows_for_rate))


@dataclass
class IngestStats:
    rows_sent: int = 0
    bytes_sent: int = 0
    batches_sent: int = 0
    errors: int = 0
    ack_latency_ms_sum: int = 0
    ack_count: int = 0
    ack_latency_max_ms: int = 0


def _log_batch_error(exc: BaseException) -> None:
    print(f"batch append/ack error: {exc!r}", file=sys.stderr, flush=True)


def _apply_ack_latency(
    latency_ms: int,
    stats: IngestStats,
    *,
    live_ack_sum: Any | None = None,
    live_ack_count: Any | None = None,
    live_ack_max: Any | None = None,
) -> None:
    stats.ack_latency_ms_sum += latency_ms
    stats.ack_count += 1
    stats.ack_latency_max_ms = max(stats.ack_latency_max_ms, latency_ms)
    if live_ack_sum is not None:
        with live_ack_sum.get_lock():
            live_ack_sum.value += latency_ms
    if live_ack_count is not None:
        with live_ack_count.get_lock():
            live_ack_count.value += 1
    if live_ack_max is not None:
        with live_ack_max.get_lock():
            live_ack_max.value = max(live_ack_max.value, latency_ms)


def _record_future_error(future: Any, stats: IngestStats) -> None:
    try:
        exc = future.exception()
        if exc is not None:
            stats.errors += 1
            _log_batch_error(exc)
    except Exception as exc:
        stats.errors += 1
        _log_batch_error(exc)


def register_batch_ack_callback(
    submit_time: float,
    future: Any,
    stats: IngestStats,
    *,
    ack_lock: threading.Lock | None = None,
    live_ack_sum: Any | None = None,
    live_ack_count: Any | None = None,
    live_ack_max: Any | None = None,
) -> None:
    """Record SDK ack latency when the append future completes (not when drained)."""

    def _on_done(done_future: Any) -> None:
        if getattr(done_future, "_ack_recorded", False):
            return
        done_future._ack_recorded = True
        latency_ms = int((time.perf_counter() - submit_time) * 1000)

        def _update() -> None:
            _record_future_error(done_future, stats)
            _apply_ack_latency(
                latency_ms,
                stats,
                live_ack_sum=live_ack_sum,
                live_ack_count=live_ack_count,
                live_ack_max=live_ack_max,
            )

        if ack_lock is not None:
            with ack_lock:
                _update()
        else:
            _update()

    future.add_done_callback(_on_done)


def apply_backpressure(
    pending: deque[Any],
    stats: IngestStats,
    max_in_flight: int,
    *,
    wait_timeout_seconds: float = 60.0,
) -> None:
    while len(pending) >= max_in_flight:
        oldest = pending[0]
        if not oldest.done():
            done, _ = concurrent.futures.wait(
                [oldest], timeout=wait_timeout_seconds
            )
            if not done:
                stats.errors += 1
                print(
                    f"backpressure wait timed out after {wait_timeout_seconds}s",
                    file=sys.stderr,
                    flush=True,
                )
                pending.popleft()
                continue
        pending.popleft()


def drain_done(pending: deque[Any]) -> None:
    for _ in range(len(pending)):
        future = pending.popleft()
        if not future.done():
            pending.append(future)


@dataclass
class IngestLoop:
    """Rate-limited append loop against one elastic channel."""

    channel: Any
    generator: ResortSalesGenerator
    row_bytes: int
    max_batch_rows: int
    target_batch_bytes: int
    max_in_flight: int
    floor_rows: int
    target_bps_fn: Callable[[], int]
    stats: IngestStats = field(default_factory=IngestStats)
    pending: deque[Any] = field(default_factory=deque)
    drain_every: int = 8
    live_ack_sum: Any | None = None
    live_ack_count: Any | None = None
    live_ack_max: Any | None = None
    ack_lock: threading.Lock = field(default_factory=threading.Lock)
    # Optional JSON fast-path (see fast_append.py / resort_sales.next_batch_ndjson):
    # pre-encode NDJSON ourselves and append via the elastic channel's FFI
    # directly, skipping the SDK's own dict->JSON re-encode.
    use_fast_append: bool = False

    def _ack_kwargs(self) -> dict[str, Any]:
        return {
            "ack_lock": self.ack_lock,
            "live_ack_sum": self.live_ack_sum,
            "live_ack_count": self.live_ack_count,
            "live_ack_max": self.live_ack_max,
        }

    def run_once(self) -> tuple[int, int, float]:
        """Append one batch; return (rows, bytes, sleep_seconds)."""
        worker_bps = max(1, self.target_bps_fn())
        rows = target_batch_rows(
            worker_bps,
            self.row_bytes,
            max_rows=self.max_batch_rows,
            target_batch_bytes=self.target_batch_bytes,
            floor_rows=self.floor_rows,
        )

        use_fast = self.use_fast_append and fast_append.supports_fast_append(
            self.channel
        )
        if use_fast:
            json_bytes, rows = self.generator.next_batch_ndjson(rows)
            batch_bytes = len(json_bytes)  # actual encoded size, not an estimate
        else:
            batch = self.generator.next_batch(rows)
            batch_bytes = rows * self.row_bytes
        tick_seconds = batch_bytes / worker_bps

        loop_start = time.perf_counter()
        apply_backpressure(self.pending, self.stats, self.max_in_flight)
        submit_time = time.perf_counter()
        if use_fast:
            # Pre-encoded bytes straight to the FFI append, bypassing the SDK's
            # own dict->JSON encode (see fast_append.py).
            future = fast_append.append_rows_ndjson(self.channel, json_bytes, rows)
        else:
            # snowpipe-streaming SDK >=1.8.0 made `StreamingIngestElasticChannel.append_rows`
            # fire-and-forget (no Future); `append_rows_with_wait` keeps the Future-based
            # interface this loop's backpressure/ack accounting relies on. The token arg
            # tracks the append for the SDK's error/success handlers — unused here since
            # ack latency/backpressure are already driven off the returned Future.
            future = self.channel.append_rows_with_wait(batch, None)
        register_batch_ack_callback(
            submit_time,
            future,
            self.stats,
            **self._ack_kwargs(),
        )
        self.pending.append(future)
        self.stats.rows_sent += rows
        self.stats.bytes_sent += batch_bytes
        self.stats.batches_sent += 1

        if self.stats.batches_sent % self.drain_every == 0:
            drain_done(self.pending)

        elapsed = time.perf_counter() - loop_start
        sleep_for = max(0.0, tick_seconds - elapsed)
        return rows, batch_bytes, sleep_for

    def flush(self, *, wait_timeout_seconds: float = 30.0) -> None:
        deadline = time.monotonic() + wait_timeout_seconds
        for future in list(self.pending):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.stats.errors += 1
                print("flush deadline exceeded", file=sys.stderr, flush=True)
                break
            if not future.done():
                done, _ = concurrent.futures.wait(
                    [future], timeout=remaining
                )
                if not done:
                    self.stats.errors += 1
                    print(
                        f"flush wait timed out after {wait_timeout_seconds}s",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
        self.pending.clear()
