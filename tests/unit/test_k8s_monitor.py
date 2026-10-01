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

"""Unit tests for the EKS cluster-resident monitor's parsing/aggregation logic.

These exercise `K8sMonitor` without a live cluster or Snowflake connection:
`_core_v1`/`_snowflake_conn` are lazily constructed, and are either never
touched (pure aggregation tests) or monkeypatched with fakes (log-tailing
tests).
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest

from elastic_channels.k8s_monitor import K8sMonitor, RunWindow


@pytest.fixture
def monitor(monkeypatch: pytest.MonkeyPatch) -> K8sMonitor:
    monkeypatch.setenv("ELASTIC_MONITOR_COMMIT_LATENCY_ENABLED", "false")
    return K8sMonitor()


def test_apply_record_sample_updates_window(monitor: K8sMonitor) -> None:
    run = RunWindow()
    monitor._apply_record(
        run,
        {
            "type": "sample",
            "worker_id": 3,
            "rows_per_sec": 1000.0,
            "uncompressed_bps": 500_000.0,
            "compressed_bps": 10_000.0,
            "avg_ack_ms": 20.0,
            "max_ack_ms": 40,
            "batches": 5,
            "errors_total": 1,
            "retryable_errors_total": 7,
        },
    )
    assert run.latest_sample[3].rows_per_sec == 1000.0
    assert run.latest_sample[3].retryable_errors_total == 7
    assert run.batches_since_flush == 5
    assert run.ack_ms_weighted_sum == pytest.approx(100.0)
    assert run.ack_count_since_flush == 5
    assert run.ack_max_ms == 40.0


def test_apply_record_start_sets_workers_expected(monitor: K8sMonitor) -> None:
    run = RunWindow()
    monitor._apply_record(run, {"type": "start", "worker_id": 0, "workers_total": 144})
    assert run.workers_expected == 144


def test_apply_record_summary_updates_worker_errors(monitor: K8sMonitor) -> None:
    run = RunWindow()
    monitor._apply_record(
        run,
        {
            "type": "sample",
            "worker_id": 1,
            "rows_per_sec": 10.0,
            "errors_total": 0,
        },
    )
    monitor._apply_record(
        run, {"type": "summary", "worker_id": 1, "errors_total": 3}
    )
    assert run.latest_sample[1].errors_total == 3


def test_flush_run_aggregates_and_writes_row(
    monitor: K8sMonitor, monkeypatch: pytest.MonkeyPatch
) -> None:
    import elastic_channels.k8s_monitor as mod

    run = RunWindow(workers_expected=2)
    now = 1_000_000.0
    monkeypatch.setattr(mod.time, "time", lambda: now)

    run.latest_sample[0] = mod.WorkerSample(
        worker_id=0,
        rows_per_sec=1000.0,
        uncompressed_bps=100.0,
        compressed_bps=10.0,
        errors_total=0,
        retryable_errors_total=2,
        seen_at=now,
    )
    run.latest_sample[1] = mod.WorkerSample(
        worker_id=1,
        rows_per_sec=2000.0,
        uncompressed_bps=200.0,
        compressed_bps=20.0,
        errors_total=1,
        retryable_errors_total=5,
        seen_at=now,
    )
    run.batches_since_flush = 10
    run.ack_ms_weighted_sum = 300.0
    run.ack_count_since_flush = 10
    run.ack_max_ms = 55.0

    captured = {}
    monitor._write_row = lambda row: captured.update(row)

    window_start = datetime.now(timezone.utc) - timedelta(seconds=30)
    window_end = datetime.now(timezone.utc)
    monitor._flush_run("run-a", run, window_start, window_end)

    assert captured["RUN_ID"] == "run-a"
    assert captured["WORKERS_REPORTING"] == 2
    assert captured["WORKERS_EXPECTED"] == 2
    assert captured["ROWS_PER_SEC"] == pytest.approx(3000.0)
    assert captured["UNCOMPRESSED_BYTES_PER_SEC"] == pytest.approx(300.0)
    assert captured["COMPRESSED_BYTES_PER_SEC"] == pytest.approx(30.0)
    assert captured["AVG_BATCH_ACK_MS"] == pytest.approx(30.0)
    assert captured["MAX_BATCH_ACK_MS"] == 55.0
    assert captured["BATCHES_ACKED"] == 10
    assert captured["ERRORS_TOTAL"] == 1
    assert captured["RETRYABLE_ERRORS_TOTAL"] == 7
    # Counters reset after flush.
    assert run.batches_since_flush == 0
    assert run.ack_count_since_flush == 0


def test_flush_run_skips_empty_window(monitor: K8sMonitor) -> None:
    run = RunWindow()
    called = []
    monitor._write_row = lambda row: called.append(row)
    now = datetime.now(timezone.utc)
    monitor._flush_run("run-b", run, now - timedelta(seconds=30), now)
    assert called == []


def test_stale_samples_excluded_from_flush(monitor: K8sMonitor) -> None:
    import elastic_channels.k8s_monitor as mod

    run = RunWindow()
    run.latest_sample[0] = mod.WorkerSample(
        worker_id=0, rows_per_sec=999.0, seen_at=0.0  # ancient
    )
    run.batches_since_flush = 1  # keep the window non-empty so it still flushes
    captured = {}
    monitor._write_row = lambda row: captured.update(row)
    now = datetime.now(timezone.utc)
    monitor._flush_run("run-c", run, now - timedelta(seconds=30), now)
    assert captured["WORKERS_REPORTING"] == 0
    assert captured["ROWS_PER_SEC"] == 0.0


def test_read_new_lines_dedupes_by_kubelet_timestamp(monitor: K8sMonitor) -> None:
    class _FakeCoreV1:
        def __init__(self, body: str) -> None:
            self.body = body

        def read_namespaced_pod_log(self, **kwargs):
            assert kwargs.get("_preload_content") is False
            return _FakeResponse(self.body.encode("utf-8"))

    class _FakeResponse:
        def __init__(self, data: bytes) -> None:
            self.data = data

    lines = (
        "2026-08-31T10:00:00.000000000Z {\"type\": \"sample\", \"worker_id\": 0}\n"
        "2026-08-31T10:00:05.000000000Z {\"type\": \"sample\", \"worker_id\": 0}\n"
    )
    monitor._core_v1_client = _FakeCoreV1(lines)
    first = monitor._read_new_lines("pod-a")
    assert len(first) == 2

    # Second poll returns the same raw log (kubelet log isn't truncated) —
    # both lines should now be considered already seen.
    second = monitor._read_new_lines("pod-a")
    assert second == []


def test_poll_once_fetches_pods_concurrently_and_applies_sequentially(
    monitor: K8sMonitor,
) -> None:
    """`_poll_once` fans I/O out across a thread pool but must still apply
    every pod's records deterministically on the main thread — verifies both
    the concurrency (all pods fetched) and correctness (state ends up right)
    halves of the refactor that replaced a fully sequential per-pod loop."""

    class _Meta:
        def __init__(self, name: str, run_id: str) -> None:
            self.name = name
            self.labels = {"run-id": run_id}

    class _Pod:
        def __init__(self, name: str, run_id: str) -> None:
            self.metadata = _Meta(name, run_id)

    class _FakeResponse:
        def __init__(self, data: bytes) -> None:
            self.data = data

    class _FakeCoreV1:
        def __init__(self, pods: list[_Pod], bodies: dict[str, str]) -> None:
            self._pods = pods
            self._bodies = bodies
            self.read_calls: list[str] = []

        def list_namespaced_pod(self, **kwargs):
            return type("Resp", (), {"items": self._pods})()

        def read_namespaced_pod_log(self, *, name: str, **kwargs):
            self.read_calls.append(name)
            return _FakeResponse(self._bodies[name].encode("utf-8"))

    pods = [_Pod("pod-a", "run-x"), _Pod("pod-b", "run-x"), _Pod("pod-c", "run-y")]
    bodies = {
        "pod-a": '2026-08-31T10:00:00.000000000Z {"type":"sample","worker_id":0,"rows_per_sec":10.0}\n',
        "pod-b": '2026-08-31T10:00:00.000000000Z {"type":"sample","worker_id":1,"rows_per_sec":20.0}\n',
        "pod-c": '2026-08-31T10:00:00.000000000Z {"type":"sample","worker_id":0,"rows_per_sec":30.0}\n',
    }
    fake = _FakeCoreV1(pods, bodies)
    monitor._core_v1_client = fake

    monitor._poll_once()

    assert sorted(fake.read_calls) == ["pod-a", "pod-b", "pod-c"]
    assert set(monitor._runs.keys()) == {"run-x", "run-y"}
    run_x = monitor._runs["run-x"]
    assert {s.rows_per_sec for s in run_x.latest_sample.values()} == {10.0, 20.0}
    run_y = monitor._runs["run-y"]
    assert run_y.latest_sample[0].rows_per_sec == 30.0
    assert monitor._pod_run_id == {"pod-a": "run-x", "pod-b": "run-x", "pod-c": "run-y"}

    # Second poll with the same raw logs — kubelet timestamp dedup should
    # mean no new samples are applied (high-water marks already advanced).
    monitor._poll_once()
    assert run_x.latest_sample[0].rows_per_sec == 10.0  # unchanged, not re-applied


def test_poll_once_does_not_block_forever_on_one_wedged_pod(monitor: K8sMonitor) -> None:
    """A single pod whose log fetch never returns (simulating the real-world
    hang that motivated the pool-bounded wait in `_poll_once`) must not stop
    every other pod's data from being applied, and `_poll_once` itself must
    return promptly rather than hanging on that one future forever."""

    class _Meta:
        def __init__(self, name: str, run_id: str) -> None:
            self.name = name
            self.labels = {"run-id": run_id}

    class _Pod:
        def __init__(self, name: str, run_id: str) -> None:
            self.metadata = _Meta(name, run_id)

    class _FakeResponse:
        def __init__(self, data: bytes) -> None:
            self.data = data

    class _FakeCoreV1:
        def __init__(self, pods: list[_Pod], bodies: dict[str, str]) -> None:
            self._pods = pods
            self._bodies = bodies

        def list_namespaced_pod(self, **kwargs):
            return type("Resp", (), {"items": self._pods})()

        def read_namespaced_pod_log(self, *, name: str, **kwargs):
            if name == "pod-wedged":
                # Long enough to outlast the test's shortened wait bound below
                # (so `_poll_once` must move on without it), short enough that
                # this background thread doesn't linger past test-suite exit.
                time.sleep(2.0)
            return _FakeResponse(self._bodies[name].encode("utf-8"))

    pods = [_Pod("pod-ok", "run-x"), _Pod("pod-wedged", "run-x")]
    bodies = {
        "pod-ok": '2026-08-31T10:00:00.000000000Z {"type":"sample","worker_id":0,"rows_per_sec":10.0}\n',
        "pod-wedged": "",
    }
    monitor._core_v1_client = _FakeCoreV1(pods, bodies)

    from elastic_channels import k8s_monitor as mod2

    original_wait = mod2.concurrent.futures.wait
    try:
        # Shrink the wait bound so the test doesn't actually sit for the real
        # ~35s deadline — behavior under test is "bounded and correct", not
        # the exact production timeout value.
        mod2.concurrent.futures.wait = lambda fs, timeout: original_wait(fs, timeout=0.2)
        started = time.monotonic()
        monitor._poll_once()
        elapsed = time.monotonic() - started
    finally:
        mod2.concurrent.futures.wait = original_wait

    assert elapsed < 5.0
    run_x = monitor._runs["run-x"]
    assert run_x.latest_sample[0].rows_per_sec == 10.0
    assert monitor._pod_last_ts.get("pod-wedged", "") == ""


def test_watchdog_force_exits_after_stall(monitor: K8sMonitor) -> None:
    """If `_last_poll_at` doesn't advance for longer than `watchdog_seconds`,
    a check must call the exit hook — verifies the safety net that recovers a
    truly wedged process via container restart, without actually killing the
    test process or depending on real sleep timing."""
    monitor.watchdog_seconds = 0.05
    calls: list[int] = []
    monitor._watchdog_exit = lambda code: calls.append(code)

    monitor._last_poll_at = time.monotonic()
    assert monitor._watchdog_check_once() is False
    assert calls == []

    monitor._last_poll_at = time.monotonic() - 10.0  # already "stalled"
    assert monitor._watchdog_check_once() is True
    assert calls == [1]
