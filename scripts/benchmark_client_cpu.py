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

"""Benchmark client-side CPU for elastic channel ramp ingest."""

from __future__ import annotations

import argparse
import cProfile
import io
import multiprocessing as mp
import os
import pstats
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from elastic_channels.ingest_loop import target_batch_rows as _target_batch_rows
from elastic_channels.resort_sales import ResortSalesGenerator, estimate_row_bytes


@dataclass
class BenchRow:
    name: str
    rows_per_sec: float
    cpu_sec: float
    wall_sec: float
    notes: str = ""

    @property
    def cpu_core_pct(self) -> float:
        return 100.0 * self.cpu_sec / max(self.wall_sec, 1e-9)


def _bench_loop(name: str, fn, *, seconds: float) -> BenchRow:
    t0 = time.perf_counter()
    cpu0 = time.process_time()
    rows = 0
    while time.perf_counter() - t0 < seconds:
        rows += fn()
    wall = time.perf_counter() - t0
    cpu = time.process_time() - cpu0
    return BenchRow(name, rows / wall, cpu, wall)


class _DoneFuture:
    __slots__ = ("_done",)

    def __init__(self) -> None:
        self._done = True

    def done(self) -> bool:
        return self._done

    def result(self) -> None:
        return None

    def exception(self) -> None:
        return None


def _mock_worker_loop(
    worker_id: int,
    *,
    seconds: float,
    worker_bps: int,
    row_bytes: int,
    max_batch_rows: int,
    max_in_flight: int,
) -> tuple[int, float, float]:
    gen = ResortSalesGenerator(seed=42, worker_id=worker_id, run_id="cpu-bench")
    pending: deque[_DoneFuture] = deque()
    rows_sent = 0
    t0 = time.perf_counter()
    cpu0 = time.process_time()

    worker_bps_local = max(1, worker_bps)
    rows_this_tick = _target_batch_rows(
        worker_bps_local,
        row_bytes,
        max_rows=max_batch_rows,
        target_batch_bytes=1_048_576,
        floor_rows=100,
    )
    gen.next_batch(rows_this_tick)  # advances generator state; batch itself unused here

    while time.perf_counter() - t0 < seconds:
        loop_start = time.perf_counter()
        batch_bytes = rows_this_tick * row_bytes
        tick_seconds = batch_bytes / worker_bps_local

        while len(pending) >= max_in_flight:
            pending.popleft()

        pending.append(_DoneFuture())
        rows_sent += rows_this_tick

        rows_next = _target_batch_rows(
            worker_bps_local,
            row_bytes,
            max_rows=max_batch_rows,
            target_batch_bytes=1_048_576,
            floor_rows=100,
        )
        gen.next_batch(rows_next)  # advances generator state; batch itself unused here
        rows_this_tick = rows_next

        elapsed = time.perf_counter() - loop_start
        sleep_for = tick_seconds - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)

    return rows_sent, time.process_time() - cpu0, time.perf_counter() - t0


def _mp_mock_workers(
    *,
    workers: int,
    seconds: float,
    aggregate_bps: int,
    row_bytes: int,
    max_batch_rows: int,
    max_in_flight: int,
) -> BenchRow:
    worker_bps = max(1, aggregate_bps // workers)
    ctx = mp.get_context("spawn")
    with ctx.Pool(processes=workers) as pool:
        wall0 = time.perf_counter()
        async_results = [
            pool.apply_async(
                _mock_worker_loop,
                kwds={
                    "worker_id": wid,
                    "seconds": seconds,
                    "worker_bps": worker_bps,
                    "row_bytes": row_bytes,
                    "max_batch_rows": max_batch_rows,
                    "max_in_flight": max_in_flight,
                },
            )
            for wid in range(workers)
        ]
        totals = [r.get() for r in async_results]
        wall = time.perf_counter() - wall0
    total_rows = sum(t[0] for t in totals)
    total_cpu = sum(t[1] for t in totals)
    return BenchRow(
        f"{workers} mock workers (gen+loop, no SDK)",
        total_rows / wall,
        total_cpu,
        wall,
        notes=f"aggregate target {aggregate_bps / 1_000_000:.0f} MB/s",
    )


def _profile_generation(batch_rows: int, iterations: int) -> str:
    gen = ResortSalesGenerator(seed=42, worker_id=0, run_id="profile")
    pr = cProfile.Profile()
    pr.enable()
    for _ in range(iterations):
        gen.next_batch(batch_rows)
    pr.disable()
    buf = io.StringIO()
    pstats.Stats(pr, stream=buf).sort_stats("cumtime").print_stats(15)
    return buf.getvalue()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=5.0)
    parser.add_argument("--workers", type=int, default=96)
    parser.add_argument("--aggregate-mbps", type=float, default=2300.0)
    parser.add_argument("--batch-rows", type=int, default=20_000)
    parser.add_argument("--max-in-flight", type=int, default=64)
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()

    row_bytes = estimate_row_bytes()
    aggregate_bps = int(args.aggregate_mbps * 1_000_000)
    worker_target_rps = (aggregate_bps // max(args.workers, 1)) // row_bytes

    print(f"CPUs={os.cpu_count()} row_bytes~={row_bytes} batch_rows={args.batch_rows}")
    print(f"Target aggregate={args.aggregate_mbps:.0f} MB/s (~{worker_target_rps:,} rows/s per worker)\n")

    gen = ResortSalesGenerator(seed=42, worker_id=0, run_id="bench")

    r_gen = _bench_loop(
        "generation only",
        lambda: args.batch_rows if gen.next_batch(args.batch_rows) else 0,
        seconds=args.seconds,
    )
    print(
        f"{r_gen.name}: {r_gen.rows_per_sec:,.0f} rows/s, "
        f"cpu={r_gen.cpu_core_pct:.0f}% of one core, "
        f"headroom={r_gen.rows_per_sec / max(worker_target_rps, 1):.1f}x vs target"
    )

    r_loop = _bench_loop(
        "generation + mock append loop (1 worker)",
        lambda: (
            _mock_worker_loop(
                0,
                seconds=0.05,
                worker_bps=aggregate_bps // max(args.workers, 1),
                row_bytes=row_bytes,
                max_batch_rows=args.batch_rows,
                max_in_flight=args.max_in_flight,
            )[0]
        ),
        seconds=args.seconds,
    )
    print(
        f"{r_loop.name}: {r_loop.rows_per_sec:,.0f} rows/s effective, "
        f"cpu={r_loop.cpu_core_pct:.0f}% of one core"
    )

    for workers in (16, 32, args.workers):
        if workers > os.cpu_count() * 4:
            continue
        r_mp = _mp_mock_workers(
            workers=workers,
            seconds=min(args.seconds, 3.0),
            aggregate_bps=aggregate_bps,
            row_bytes=row_bytes,
            max_batch_rows=args.batch_rows,
            max_in_flight=args.max_in_flight,
        )
        core_util = r_mp.cpu_sec / max(r_mp.wall_sec * os.cpu_count(), 1e-9)
        print(
            f"{r_mp.name}: {r_mp.rows_per_sec:,.0f} rows/s, "
            f"cpu={r_mp.cpu_sec:.1f}s wall={r_mp.wall_sec:.1f}s "
            f"({core_util * 100:.0f}% of {os.cpu_count()} cores) · {r_mp.notes}"
        )

    print("\nConclusions:")
    headroom = r_gen.rows_per_sec / max(worker_target_rps, 1)
    if headroom >= 5:
        print("- Row generation is NOT the CPU bottleneck; SDK serialize/network dominates.")
        print("- To use less CPU: try fewer workers + higher MAX_IN_FLIGHT, or smaller row payloads.")
    else:
        print("- Generation may be CPU-bound; optimize TXID formatting and batch sizing.")

    if args.profile:
        print("\ncProfile (top 15, single-worker generation):")
        print(_profile_generation(args.batch_rows, 200))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
