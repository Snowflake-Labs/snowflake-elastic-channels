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

"""Unit tests for elastic channel ingest/generator/SDK-stats helpers."""

import pytest

from elastic_channels.ingest_loop import (
    IngestStats,
    register_batch_ack_callback,
    target_batch_rows,
)
from elastic_channels.resort_sales import ResortSalesGenerator, estimate_row_bytes
from elastic_channels.sdk_stats import compute_delta, parse_prometheus_text


def test_resort_sales_generator_deterministic() -> None:
    g1 = ResortSalesGenerator(seed=99, worker_id=0)
    g2 = ResortSalesGenerator(seed=99, worker_id=0)
    r1 = g1.next_record()
    r2 = g2.next_record()
    for key in ("RESORT", "CATEGORY", "ITEM", "CUSTOMER_TYPE", "PAYMENT_METHOD", "UNIT_PRICE"):
        assert r1[key] == r2[key]


def test_resort_sales_batch_size() -> None:
    gen = ResortSalesGenerator(seed=1, worker_id=2)
    batch = gen.next_batch(10, run_id="run-a", worker_id=2)
    assert len(batch) == 10
    assert all(row["RUN_ID"] == "run-a" and row["WORKER_ID"] == 2 for row in batch)
    assert all(row["AMOUNT"] == round(row["QUANTITY"] * row["UNIT_PRICE"], 2) for row in batch)


def test_target_batch_rows_scales_with_throughput() -> None:
    row_bytes = 195
    low = target_batch_rows(
        1_000_000,
        row_bytes,
        max_rows=10_000,
        target_batch_bytes=1_048_576,
        floor_rows=100,
    )
    high = target_batch_rows(
        1_000_000_000,
        row_bytes,
        max_rows=10_000,
        target_batch_bytes=1_048_576,
        floor_rows=100,
    )
    assert high > low
    assert high <= 10_000


def test_estimate_row_bytes_reasonable() -> None:
    avg = estimate_row_bytes()
    assert 150 <= avg <= 2000


def test_batch_ack_callback_records_at_completion(monkeypatch) -> None:
    class DelayedFuture:
        def __init__(self) -> None:
            self._done = False
            self._callbacks: list = []

        def done(self) -> bool:
            return self._done

        def exception(self) -> None:
            return None

        def add_done_callback(self, fn) -> None:
            self._callbacks.append(fn)

        def complete(self) -> None:
            self._done = True
            for fn in list(self._callbacks):
                fn(self)

    times = iter([10.05])
    monkeypatch.setattr("elastic_channels.ingest_loop.time.perf_counter", lambda: next(times))

    stats = IngestStats()
    future = DelayedFuture()
    register_batch_ack_callback(10.0, future, stats)
    assert stats.ack_count == 0

    future.complete()
    assert stats.ack_count == 1
    assert stats.ack_latency_ms_sum == 50
    assert stats.ack_latency_max_ms == 50


_SAMPLE_PROMETHEUS_BODY = """\
# TYPE input_payload_bytes summary
input_payload_bytes{client_name="w0",channel_name="ELASTIC",quantile="0.5"} 108521.1
input_payload_bytes_sum{client_name="w0",channel_name="ELASTIC"} 2170446
input_payload_bytes_count{client_name="w0",channel_name="ELASTIC"} 20
# TYPE output_payload_compressed_bytes summary
output_payload_compressed_bytes_sum{client_name="w0",channel_name="ELASTIC"} 63966
output_payload_compressed_bytes_count{client_name="w0",channel_name="ELASTIC"} 20
# TYPE input_buffer_bytes gauge
input_buffer_bytes{client_name="w0",channel_name="ELASTIC"} 4096
input_buffer_byte_limit_reached_count{client_name="w0",channel_name="ELASTIC"} 3
sf_api_retryable_error_count{client_name="w0",channel_name="ELASTIC",api_name="insert_rows"} 1
sf_api_retryable_error_count{client_name="w0",channel_name="ELASTIC",api_name="insert_files"} 0
"""


def test_parse_prometheus_text_extracts_sum_and_gauges() -> None:
    values = parse_prometheus_text(_SAMPLE_PROMETHEUS_BODY)
    assert values["input_payload_bytes_sum"] == 2170446
    assert values["output_payload_compressed_bytes_sum"] == 63966
    assert values["input_buffer_bytes"] == 4096
    assert values["input_buffer_byte_limit_reached_count"] == 3
    # Multiple label-series for the same metric name are summed.
    assert values["sf_api_retryable_error_count"] == 1


def test_compute_delta_bytes_per_second() -> None:
    prev = {
        "input_payload_bytes_sum": 1_000_000,
        "output_payload_compressed_bytes_sum": 30_000,
        "input_buffer_bytes": 0,
        "input_buffer_byte_limit_reached_count": 0,
        "sf_api_retryable_error_count": 0,
    }
    curr = {
        "input_payload_bytes_sum": 6_000_000,
        "output_payload_compressed_bytes_sum": 180_000,
        "input_buffer_bytes": 2048,
        "input_buffer_byte_limit_reached_count": 2,
        "sf_api_retryable_error_count": 1,
    }
    delta = compute_delta(prev, curr, elapsed_seconds=5.0)
    assert delta is not None
    assert delta.uncompressed_bps == 1_000_000  # (6M - 1M) / 5s
    assert delta.compressed_bps == 30_000  # (180k - 30k) / 5s
    assert delta.compression_ratio == pytest.approx(1_000_000 / 30_000)
    assert delta.input_buffer_bytes == 2048
    assert delta.buffer_limit_reached_total == 2
    assert delta.retryable_errors_total == 1


def test_compute_delta_rejects_counter_reset() -> None:
    prev = {"input_payload_bytes_sum": 5_000_000, "output_payload_compressed_bytes_sum": 100}
    curr = {"input_payload_bytes_sum": 1_000, "output_payload_compressed_bytes_sum": 50}
    assert compute_delta(prev, curr, elapsed_seconds=5.0) is None


def test_compute_delta_zero_elapsed_returns_none() -> None:
    assert compute_delta({}, {}, elapsed_seconds=0.0) is None


def test_compute_delta_missing_uncompressed_key_returns_none() -> None:
    """`input_payload_bytes` missing from a scrape means no usable data at all."""
    prev = {"output_payload_compressed_bytes_sum": 100}
    curr = {"input_payload_bytes_sum": 6_000_000, "output_payload_compressed_bytes_sum": 200}
    assert compute_delta(prev, curr, elapsed_seconds=5.0) is None


def test_compute_delta_evicted_compressed_metric_keeps_uncompressed() -> None:
    """Regression test: the SDK's exporter evicts idle `output_payload_compressed_bytes`
    from `/metrics` entirely under sustained load (confirmed live 2026-07-24) while
    `input_payload_bytes` keeps updating. A missing compressed key must not be treated
    as a real `0.0` — that would fabricate a fake "compressed throughput collapsed"
    reading — but the still-valid uncompressed rate must not be thrown away either."""
    prev = {"input_payload_bytes_sum": 1_000_000}
    curr = {"input_payload_bytes_sum": 6_000_000}
    delta = compute_delta(prev, curr, elapsed_seconds=5.0)
    assert delta is not None
    assert delta.uncompressed_bps == pytest.approx(1_000_000)
    assert delta.compressed_bps == 0.0
    assert delta.compression_ratio == 0.0
    assert delta.compression_metrics_available is False


def test_compute_delta_compressed_counter_reset_marks_unavailable() -> None:
    """If compressed goes backwards (evicted-then-recreated with a fresh baseline)
    while uncompressed is healthy, keep the uncompressed rate but flag compression as
    unavailable rather than returning a nonsensical negative/zero compressed delta."""
    prev = {"input_payload_bytes_sum": 1_000_000, "output_payload_compressed_bytes_sum": 90_000}
    curr = {"input_payload_bytes_sum": 6_000_000, "output_payload_compressed_bytes_sum": 50}
    delta = compute_delta(prev, curr, elapsed_seconds=5.0)
    assert delta is not None
    assert delta.uncompressed_bps == pytest.approx(1_000_000)
    assert delta.compressed_bps == 0.0
    assert delta.compression_metrics_available is False


def test_compute_delta_sums_rowset_and_file_mode_compressed_bytes() -> None:
    """Root-cause regression test (2026-07-24): the SDK's ModeSelector routes each
    batch through either inline rowset mode (`output_payload_compressed_bytes`) or
    file-fragment mode (`output_file_compressed_bytes`) depending on backpressure —
    never both — so a channel that's mixing modes across an interval must have both
    counters summed to get the true compressed total, not just whichever one this
    module originally tracked."""
    prev = {
        "input_payload_bytes_sum": 1_000_000,
        "output_payload_compressed_bytes_sum": 10_000,
        "output_file_compressed_bytes_sum": 40_000,
    }
    curr = {
        "input_payload_bytes_sum": 6_000_000,
        "output_payload_compressed_bytes_sum": 30_000,  # +20,000 (rowset mode)
        "output_file_compressed_bytes_sum": 140_000,  # +100,000 (file-fragment mode)
    }
    delta = compute_delta(prev, curr, elapsed_seconds=5.0)
    assert delta is not None
    assert delta.compressed_bps == pytest.approx(24_000.0)  # (20,000 + 100,000) / 5s
    assert delta.compression_metrics_available is True


def test_compute_delta_file_mode_only_still_reports_compression() -> None:
    """When a channel is pinned entirely in file-fragment mode (the common case at
    sustained high throughput — see module docstring), `output_payload_compressed_bytes`
    is absent/evicted but `output_file_compressed_bytes` alone must still produce a
    real, non-zero compressed rate instead of falling back to the old "no data"
    behavior that only checked the rowset-mode counter."""
    prev = {"input_payload_bytes_sum": 1_000_000, "output_file_compressed_bytes_sum": 40_000}
    curr = {"input_payload_bytes_sum": 6_000_000, "output_file_compressed_bytes_sum": 140_000}
    delta = compute_delta(prev, curr, elapsed_seconds=5.0)
    assert delta is not None
    assert delta.uncompressed_bps == pytest.approx(1_000_000)
    assert delta.compressed_bps == pytest.approx(20_000.0)  # 100,000 / 5s
    assert delta.compression_ratio == pytest.approx(5_000_000 / 100_000)  # uncomp/comp delta
    assert delta.compression_metrics_available is True


def test_compute_delta_both_compressed_metrics_missing_marks_unavailable() -> None:
    """Only when *both* rowset- and file-mode compressed counters are absent should
    compression be reported as unavailable."""
    prev = {"input_payload_bytes_sum": 1_000_000}
    curr = {"input_payload_bytes_sum": 6_000_000}
    delta = compute_delta(prev, curr, elapsed_seconds=5.0)
    assert delta is not None
    assert delta.compressed_bps == 0.0
    assert delta.compression_metrics_available is False
