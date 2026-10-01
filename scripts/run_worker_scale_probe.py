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

"""Incrementally scale EKS worker count up or down, stopping on
throttling/errors or a throughput plateau.

Each step runs a short fixed-throughput job (`infra/eks/run_job.sh`) with a
worker count and a *per-worker* target rate set high enough (`--worker-bps`,
default well above any single worker's realistic max) that the client-side
rate limiter in `IngestLoop.run_once()` is never the bottleneck — so the
measured aggregate throughput reflects real capacity (SDK/network/Snowflake),
not an artificial cap. Worker count is the only variable that changes between
steps.

**`--direction up` (default):** scale-up probe. After each step, the step is
scored from `ELASTIC_K8S_METRICS`:
  - unhealthy  -> ERRORS_TOTAL or RETRYABLE_ERRORS_TOTAL (SDK 429/503 retries,
                  i.e. throttling) increased during the step. Stop; the
                  *previous* step is the last known-good worker count.
  - plateaued  -> healthy, but aggregate uncompressed_bps grew less than
                  `--growth-threshold-pct` versus the previous step. Stop;
                  *this* step is still healthy and is reported as the peak.
  - growing    -> healthy and grew enough. Scale up by `--step-workers` and
                  continue, up to `--max-workers`.

**`--direction down`:** scale-down probe, for finding the highest clean
worker count below a known-unhealthy starting point (e.g. after a scale-up
probe or config change — such as `--fast-json-encode` — pushed throughput
into throttling). Steps down by `--step-workers` from `--start-workers` while
a step is unhealthy; stops and reports the peak as soon as a step is healthy
(no errors, no retryable errors) — no growth/plateau scoring in this
direction, since the goal is finding the clean ceiling, not more throughput.

Worker nodes are kept up (`--keep-nodes`) between steps to avoid re-scaling
per step; call `infra/eks/teardown_workers.sh` yourself when done (or pass
--teardown-on-stop).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

import os

# Supported worker-count ceiling (WORKER_COUNT_MAX in infra/eks/lib/eks_env.sh).
DEFAULT_MAX_WORKERS = 455


def run(cmd: list[str]) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(cmd)}", flush=True)
    return subprocess.run(cmd, cwd=ROOT, check=False)


def run_step(
    run_id: str,
    *,
    workers: int,
    worker_bps: int,
    duration_seconds: int,
    max_batch_rows: int,
    target_batch_bytes: int,
    max_in_flight: int,
    pipe_name: str,
    fast_json_encode: bool,
) -> bool:
    """Submit and wait for one step's Job. Returns False if run_job.sh itself failed
    (e.g. the Job never reached Complete) — treated as unhealthy regardless of
    whatever partial metrics made it to Snowflake."""
    total_bps = workers * worker_bps
    proc = run(
        [
            "./infra/eks/run_job.sh",
            run_id,
            "--target-bps",
            str(total_bps),
            "--duration-seconds",
            str(duration_seconds),
            "--workers",
            str(workers),
            "--max-batch-rows",
            str(max_batch_rows),
            "--target-batch-bytes",
            str(target_batch_bytes),
            "--max-in-flight",
            str(max_in_flight),
            "--pipe-name",
            pipe_name,
            "--fast-json-encode",
            "true" if fast_json_encode else "false",
            "--keep-nodes",
        ]
    )
    return proc.returncode == 0


def cleanup_step_job(run_id: str) -> None:
    """Delete this step's Job (cascades to its pods) once we're done scoring it.

    Without this, completed Jobs/pods pile up across steps (Jobs have no default
    TTL) and the monitor's `_list_worker_pods()` + per-pod sequential log-tail
    ends up listing/tailing *every* prior step's pods too, not just the current
    run's — the lag grows roughly with cumulative pods ever created, not just
    the current run's worker count, and can eventually starve new runs of any
    metrics at all."""
    subprocess.run(
        [
            "kubectl",
            "-n",
            "elastic-ramp",
            "delete",
            "job",
            f"elastic-ramp-{run_id}",
            "--ignore-not-found",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
    )


def fetch_step_metrics(run_id: str, *, warmup_seconds: float) -> dict | None:
    """Aggregate this run's ELASTIC_K8S_METRICS rows, skipping the first
    `warmup_seconds` of windows (ramp-up: channel open, first batches) — mirrors
    how the sustain-test analysis already drops warm-up windows. Cumulative
    counters (errors) are read from the last row."""
    import snowflake.connector

    from elastic_channels.auth import connector_config

    database = os.environ.get("SNOWFLAKE_DATABASE", "SC_DB")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "SC_SCHEMA")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "XSMALL_WH")

    cfg = connector_config()
    with snowflake.connector.connect(**cfg) as conn:
        cur = conn.cursor()
        cur.execute(f"USE WAREHOUSE {warehouse}")
        cur.execute(
            f"""
            SELECT WINDOW_START, WORKERS_REPORTING, WORKERS_EXPECTED, ROWS_PER_SEC,
                   UNCOMPRESSED_BYTES_PER_SEC, COMPRESSED_BYTES_PER_SEC,
                   ERRORS_TOTAL, RETRYABLE_ERRORS_TOTAL
            FROM {database}.{schema}.ELASTIC_K8S_METRICS
            WHERE RUN_ID = %s
            ORDER BY WINDOW_START
            """,
            (run_id,),
        )
        rows = cur.fetchall()
    if not rows:
        return None

    first_ts = rows[0][0]
    scored = [r for r in rows if (r[0] - first_ts).total_seconds() >= warmup_seconds]
    if not scored:
        scored = rows[-1:]

    uncompressed = [r[4] for r in scored if r[4] is not None]
    rows_per_sec = [r[3] for r in scored if r[3] is not None]
    last = rows[-1]
    return {
        "windows": len(rows),
        "scored_windows": len(scored),
        "workers_reporting_last": last[1],
        "workers_expected": last[2],
        "avg_rows_per_sec": sum(rows_per_sec) / len(rows_per_sec) if rows_per_sec else 0.0,
        "avg_uncompressed_bps": sum(uncompressed) / len(uncompressed) if uncompressed else 0.0,
        "errors_total": last[6] or 0,
        "retryable_errors_total": last[7] or 0,
    }


def fetch_step_metrics_from_pod_logs(run_id: str) -> dict | None:
    """Fallback for when the monitor's Snowflake-aggregated row never lands.

    Even after parallelizing `K8sMonitor._poll_once` and adding a watchdog, the
    monitor can still occasionally wedge and force-restart mid-run, missing
    that run's whole log window — losing the step's data even though the
    ingest job itself completed cleanly. This reconstructs the same shape of
    result directly from each worker pod's own stdout JSON, which is
    authoritative regardless of monitor health: last `type=sample` line per
    worker (steady-state rate right before shutdown), summed across all
    workers. Only usable once the job's pods still exist (before cleanup)."""
    proc = subprocess.run(
        [
            "kubectl",
            "-n",
            "elastic-ramp",
            "logs",
            "-l",
            f"run-id={run_id}",
            "--prefix",
            "--tail=-1",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0 or not proc.stdout:
        return None

    import json as _json
    import re as _re

    per_worker_last: dict[int, dict] = {}
    for line in proc.stdout.splitlines():
        m = _re.match(r"^\[[^\]]+\]\s+(\{.*\})\s*$", line.strip())
        if not m:
            continue
        try:
            rec = _json.loads(m.group(1))
        except (ValueError, TypeError):
            continue
        if rec.get("type") == "sample" and "worker_id" in rec:
            per_worker_last[rec["worker_id"]] = rec

    if not per_worker_last:
        return None

    samples = list(per_worker_last.values())
    return {
        "windows": 1,
        "scored_windows": 1,
        "workers_reporting_last": len(samples),
        "workers_expected": len(samples),
        "avg_rows_per_sec": sum(s.get("rows_per_sec", 0.0) for s in samples),
        "avg_uncompressed_bps": sum(s.get("uncompressed_bps", 0.0) for s in samples),
        "errors_total": sum(s.get("errors_total", 0) for s in samples),
        "retryable_errors_total": sum(s.get("retryable_errors_total", 0) for s in samples),
        "source": "pod-logs-fallback",
    }


def fmt_bps(bps: float) -> str:
    if bps >= 1_073_741_824:
        return f"{bps / 1_073_741_824:.2f} GiB/s"
    if bps >= 1_048_576:
        return f"{bps / 1_048_576:.2f} MiB/s"
    return f"{bps:.0f} B/s"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", default="scaleprobe", help="Run-id prefix")
    parser.add_argument(
        "--direction",
        choices=["up", "down"],
        default="up",
        help="'up': scale-up probe (default). 'down': scale-down probe from "
        "--start-workers until a step is healthy — for finding the clean "
        "ceiling below a known-unhealthy starting point.",
    )
    parser.add_argument("--start-workers", type=int, default=21)
    parser.add_argument("--step-workers", type=int, default=21)
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    parser.add_argument(
        "--min-workers",
        type=int,
        default=1,
        help="Floor for --direction down (stop and report failure if reached without a healthy step)",
    )
    parser.add_argument(
        "--fast-json-encode",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Pass --fast-json-encode to every step's run_job.sh call (default: "
        "true, matching run_job.sh's own default; pass --no-fast-json-encode "
        "to probe the SDK's default encode path instead)",
    )
    parser.add_argument(
        "--worker-bps",
        type=int,
        default=100_000_000,
        help="Per-worker target bytes/sec, set well above any single worker's "
        "realistic max so the client-side rate limiter is never the bottleneck",
    )
    parser.add_argument("--duration-seconds", type=int, default=90)
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=25.0,
        help="Skip this many seconds of windows at the start of each step when scoring",
    )
    parser.add_argument("--growth-threshold-pct", type=float, default=3.0)
    parser.add_argument("--metrics-wait-seconds", type=float, default=20.0)
    parser.add_argument("--metrics-wait-attempts", type=int, default=6)
    parser.add_argument("--max-batch-rows", type=int, default=40_000)
    parser.add_argument("--target-batch-bytes", type=int, default=2_097_152)
    parser.add_argument("--max-in-flight", type=int, default=64)
    parser.add_argument("--pipe-name", default=os.environ.get("ELASTIC_PIPE_NAME", "RESORT_SALES_PIPE"))
    parser.add_argument(
        "--teardown-on-stop",
        action="store_true",
        help="Scale worker nodes to zero once the probe stops (default: leave nodes up)",
    )
    parser.add_argument(
        "--seed-workers",
        type=int,
        default=None,
        help="Resume from a prior probe run: worker count of the last known-good step "
        "(paired with --seed-uncompressed-bps) so the first new step's growth is still "
        "scored against it instead of running unconditionally",
    )
    parser.add_argument(
        "--seed-uncompressed-bps",
        type=float,
        default=None,
        help="Resume from a prior probe run: avg_uncompressed_bps of the last known-good step",
    )
    args = parser.parse_args()

    history: list[dict] = []
    workers = args.start_workers
    last_good: dict | None = None
    if args.seed_workers is not None and args.seed_uncompressed_bps is not None:
        last_good = {
            "workers": args.seed_workers,
            "avg_uncompressed_bps": args.seed_uncompressed_bps,
            "run_id": "(seeded from prior probe)",
            "avg_rows_per_sec": 0.0,
        }
        print(
            f"Seeded baseline: {args.seed_workers} workers, "
            f"{fmt_bps(args.seed_uncompressed_bps)} uncompressed"
        )

    while (workers <= args.max_workers) if args.direction == "up" else (workers >= args.min_workers):
        run_id = f"{args.label}-{datetime.now(timezone.utc):%Y%m%d%H%M%S}-w{workers}"
        print(f"\n=== step: {workers} workers (run_id={run_id}) ===", flush=True)

        ok = run_step(
            run_id,
            workers=workers,
            worker_bps=args.worker_bps,
            duration_seconds=args.duration_seconds,
            max_batch_rows=args.max_batch_rows,
            target_batch_bytes=args.target_batch_bytes,
            max_in_flight=args.max_in_flight,
            pipe_name=args.pipe_name,
            fast_json_encode=args.fast_json_encode,
        )
        # Metrics land a few seconds after the Job completes (monitor's poll +
        # flush cadence) — but the monitor tails every worker pod's log
        # *sequentially* each poll, so at higher worker counts a full poll
        # cycle can take longer than ELASTIC_MONITOR_POLL_SECONDS, delaying
        # the first flush for a run well past job completion. Poll Snowflake
        # instead of assuming a fixed delay.
        metrics = None
        for attempt in range(args.metrics_wait_attempts):
            time.sleep(args.metrics_wait_seconds)
            metrics = fetch_step_metrics(run_id, warmup_seconds=args.warmup_seconds)
            if metrics is not None:
                break
            print(
                f"...no metrics rows yet for {run_id} "
                f"(attempt {attempt + 1}/{args.metrics_wait_attempts}), waiting"
            )

        if metrics is None:
            print(f"...falling back to direct pod-log aggregation for {run_id}")
            metrics = fetch_step_metrics_from_pod_logs(run_id)
            if metrics is not None:
                print(
                    "...recovered metrics directly from pod logs "
                    "(monitor did not flush this run to Snowflake)"
                )

        if metrics is not None:
            # Only safe to delete once we've confirmed the monitor already
            # flushed this run — deleting on a still-pending/timed-out step
            # races the monitor's in-progress log tail and can destroy pods
            # (and their logs) before it ever catches up, permanently losing
            # the step's data instead of just delaying it.
            cleanup_step_job(run_id)

        if not ok or metrics is None:
            print(
                f"STOP: step at {workers} workers failed to complete or produced no "
                f"metrics (job_ok={ok}, metrics={metrics})"
            )
            break

        metrics["run_id"] = run_id
        metrics["workers"] = workers
        history.append(metrics)
        print(
            f"result: workers_reporting={metrics['workers_reporting_last']}/"
            f"{metrics['workers_expected']} rows/s={metrics['avg_rows_per_sec']:.0f} "
            f"uncompressed={fmt_bps(metrics['avg_uncompressed_bps'])} "
            f"errors_total={metrics['errors_total']} "
            f"retryable_errors_total={metrics['retryable_errors_total']}"
        )

        unhealthy = metrics["errors_total"] > 0 or metrics["retryable_errors_total"] > 0

        if args.direction == "down":
            if unhealthy:
                print(
                    f"unhealthy at {workers} workers — "
                    f"errors_total={metrics['errors_total']} "
                    f"retryable_errors_total={metrics['retryable_errors_total']} "
                    f"(throttling/4xx). Stepping down by {args.step_workers}."
                )
                workers -= args.step_workers
                continue
            print(
                f"STOP: healthy at {workers} workers — no errors, no retryable "
                "errors. Reporting as the clean peak for this configuration."
            )
            last_good = metrics
            break

        if unhealthy:
            print(
                f"STOP: unhealthy at {workers} workers — "
                f"errors_total={metrics['errors_total']} "
                f"retryable_errors_total={metrics['retryable_errors_total']} "
                f"(throttling/4xx). Last known-good: "
                f"{last_good['workers'] if last_good else 'none'}"
            )
            break

        if last_good is not None:
            prev_bps = last_good["avg_uncompressed_bps"]
            growth_pct = (
                (metrics["avg_uncompressed_bps"] - prev_bps) / prev_bps * 100
                if prev_bps > 0
                else 100.0
            )
            print(f"growth vs previous step: {growth_pct:+.1f}%")
            if growth_pct < args.growth_threshold_pct:
                print(
                    f"STOP: plateaued at {workers} workers — throughput grew only "
                    f"{growth_pct:+.1f}% (< {args.growth_threshold_pct}% threshold) "
                    f"vs {last_good['workers']} workers. This step is healthy; "
                    "reporting it as the peak."
                )
                last_good = metrics
                break

        last_good = metrics
        workers += args.step_workers

    print("\n=== summary ===")
    print(f"{'workers':>8} {'rows/s':>12} {'uncompressed':>14} {'errors':>8} {'retryable':>10}")
    for m in history:
        print(
            f"{m['workers']:>8} {m['avg_rows_per_sec']:>12.0f} "
            f"{fmt_bps(m['avg_uncompressed_bps']):>14} {m['errors_total']:>8} "
            f"{m['retryable_errors_total']:>10}"
        )
    if last_good is not None:
        print(
            f"\nPeak healthy: {last_good['workers']} workers, "
            f"{fmt_bps(last_good['avg_uncompressed_bps'])} uncompressed, "
            f"{last_good['avg_rows_per_sec']:.0f} rows/s "
            f"(run_id={last_good['run_id']})"
        )

    if args.teardown_on_stop:
        run(["./infra/eks/teardown_workers.sh"])
    else:
        print("\nWorker nodes left up (--teardown-on-stop not passed) — "
              "run ./infra/eks/teardown_workers.sh when done.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
