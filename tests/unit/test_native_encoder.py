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

"""Tests for the optional JSON fast-path (native NDJSON encoder + FFI append
bypass) — see `elastic_channels/resort_sales.py` and `fast_append.py`.

The `elastic_channels_native` Rust wheel is optional and not guaranteed to be
built in every test environment (it requires a Rust toolchain + maturin — see
`native/`), so native-specific assertions are skipped when it isn't
importable. The msgspec fallback path always runs.
"""

from __future__ import annotations

import json
from concurrent.futures import Future

import pytest

from elastic_channels import fast_append
from elastic_channels.resort_sales import ResortSalesGenerator, native_encoder_available


def test_next_batch_ndjson_fallback_matches_next_batch_json() -> None:
    """Without the native wheel, `next_batch_ndjson` must encode the exact same
    rows `next_batch` would build, just pre-serialized."""
    gen_a = ResortSalesGenerator(seed=1, worker_id=5, run_id="run-x")
    gen_b = ResortSalesGenerator(seed=1, worker_id=5, run_id="run-x")

    batch = gen_a.next_batch(25)
    payload, num_rows = gen_b.next_batch_ndjson(25)

    assert num_rows == 25
    lines = payload.decode("utf-8").splitlines()
    assert len(lines) == 25
    for row, line in zip(batch, lines):
        assert json.loads(line) == row
    # Both generators must land on the same internal counter afterward so a
    # loop that mixes next_batch/next_batch_ndjson calls stays deterministic.
    assert gen_a._counter == gen_b._counter


def test_next_batch_ndjson_empty_batch() -> None:
    gen = ResortSalesGenerator(seed=1, worker_id=0, run_id="run-empty")
    payload, num_rows = gen.next_batch_ndjson(0)
    assert num_rows == 0
    assert payload == b""


def test_next_batch_ndjson_respects_run_id_and_worker_id_overrides() -> None:
    gen = ResortSalesGenerator(seed=1, worker_id=1, run_id="run-a")
    payload, _ = gen.next_batch_ndjson(3, run_id="run-b", worker_id=9)
    for line in payload.decode("utf-8").splitlines():
        row = json.loads(line)
        assert row["RUN_ID"] == "run-b"
        assert row["WORKER_ID"] == 9


@pytest.mark.skipif(
    not native_encoder_available(), reason="elastic_channels_native wheel not installed"
)
def test_native_encoder_matches_fallback_encoder() -> None:
    """Native and fallback paths must produce byte-identical NDJSON for the
    same starting state, so switching ELASTIC_FAST_JSON_ENCODE never changes
    what lands in Snowflake."""
    native_gen = ResortSalesGenerator(seed=1, worker_id=4, run_id="run-native", use_native=True)
    fallback_gen = ResortSalesGenerator(seed=1, worker_id=4, run_id="run-native")

    native_payload, native_rows = native_gen.next_batch_ndjson(37)
    fallback_payload, fallback_rows = fallback_gen.next_batch_ndjson(37)

    assert native_rows == fallback_rows == 37
    assert native_payload == fallback_payload
    assert native_gen._counter == fallback_gen._counter


@pytest.mark.skipif(
    not native_encoder_available(), reason="elastic_channels_native wheel not installed"
)
def test_native_encoder_available_reports_true() -> None:
    assert native_encoder_available() is True


class _FakeCallback:
    def __init__(self) -> None:
        self.registered: list[tuple[int, object, object]] = []
        self.removed: list[int] = []

    def register_future(self, future_id, future, append_token):
        self.registered.append((future_id, future, append_token))

    def remove_future(self, future_id) -> None:
        self.removed.append(future_id)


class _FakeRustChannel:
    def __init__(self, *, raise_on_append: BaseException | None = None) -> None:
        self.calls: list[tuple[bytes, int, int, object, object]] = []
        self._raise_on_append = raise_on_append

    def append_rows(self, json_bytes, num_rows, future_id, start_token, end_token):
        if self._raise_on_append is not None:
            raise self._raise_on_append
        self.calls.append((json_bytes, num_rows, future_id, start_token, end_token))

    def invalidate_channel(self, message) -> None:
        pass


class _FakeElasticChannel:
    """Stand-in exposing the same private surface as
    `StreamingIngestElasticChannel` that `fast_append.py` relies on."""

    def __init__(self, *, raise_on_append: BaseException | None = None) -> None:
        import threading

        self._future_id_lock = threading.Lock()
        self._next_future_id = 1
        self._callback = _FakeCallback()
        self._channel = _FakeRustChannel(raise_on_append=raise_on_append)


def test_supports_fast_append_detects_elastic_channel() -> None:
    assert fast_append.supports_fast_append(_FakeElasticChannel()) is True


def test_supports_fast_append_rejects_unsupported_channel() -> None:
    class _FakeUnsupportedChannel:
        channel_name = "N0_W0"

    assert fast_append.supports_fast_append(_FakeUnsupportedChannel()) is False


def test_append_rows_ndjson_registers_future_and_calls_ffi() -> None:
    channel = _FakeElasticChannel()
    future = fast_append.append_rows_ndjson(channel, b'{"A":1}\n', 1, append_token="tok")

    assert channel._callback.registered == [(1, future, "tok")]
    assert channel._channel.calls == [(b'{"A":1}\n', 1, 1, None, None)]
    assert channel._next_future_id == 2


def test_append_rows_ndjson_increments_future_id_across_calls() -> None:
    channel = _FakeElasticChannel()
    fast_append.append_rows_ndjson(channel, b"{}\n", 1)
    fast_append.append_rows_ndjson(channel, b"{}\n", 1)
    ids = [call[2] for call in channel._channel.calls]
    assert ids == [1, 2]


def test_append_rows_ndjson_zero_rows_short_circuits() -> None:
    channel = _FakeElasticChannel()
    future = fast_append.append_rows_ndjson(channel, b"", 0)
    assert future.done() and future.exception() is None
    assert channel._channel.calls == []
    assert channel._callback.registered == []


def test_append_rows_ndjson_cleans_up_future_on_ffi_error() -> None:
    channel = _FakeElasticChannel(raise_on_append=RuntimeError("boom"))
    with pytest.raises(RuntimeError):
        fast_append.append_rows_ndjson(channel, b"{}\n", 1)
    assert channel._callback.removed == [1]


def test_ingest_loop_run_once_uses_fast_append_when_enabled() -> None:
    """`IngestLoop.run_once(use_fast_append=True)` against a channel that
    supports the fast path must skip `append_rows_with_wait` entirely and go
    straight through `fast_append.append_rows_ndjson`, reporting the real
    encoded byte count rather than the `rows * row_bytes` estimate."""
    from elastic_channels.ingest_loop import IngestLoop

    class _NoAppendRowsWithWait(_FakeElasticChannel):
        def append_rows_with_wait(self, *args, **kwargs):  # pragma: no cover
            raise AssertionError(
                "append_rows_with_wait must not be called when use_fast_append=True"
            )

    channel = _NoAppendRowsWithWait()
    generator = ResortSalesGenerator(seed=1, worker_id=0, run_id="run-fast")
    loop = IngestLoop(
        channel=channel,
        generator=generator,
        row_bytes=200,
        max_batch_rows=1000,
        target_batch_bytes=1_048_576,
        max_in_flight=8,
        floor_rows=10,
        target_bps_fn=lambda: 1_000_000,
        use_fast_append=True,
    )

    rows, batch_bytes, _sleep_for = loop.run_once()

    assert rows > 0
    assert len(channel._channel.calls) == 1
    json_bytes, num_rows, *_ = channel._channel.calls[0]
    assert num_rows == rows
    assert batch_bytes == len(json_bytes)  # real size, not the row_bytes estimate
    assert loop.stats.rows_sent == rows
    assert loop.stats.bytes_sent == batch_bytes


def test_ingest_loop_run_once_falls_back_for_unsupported_channel() -> None:
    """`use_fast_append=True` on a channel that doesn't support the FFI bypass
    must silently use the normal `append_rows_with_wait` path instead of
    erroring."""
    from elastic_channels.ingest_loop import IngestLoop

    class _FakeChannelWithoutFastAppend:
        channel_name = "N0_W0"

        def append_rows_with_wait(self, rows, append_token=None):
            future: Future = Future()
            future.set_result(None)
            return future

    channel = _FakeChannelWithoutFastAppend()
    generator = ResortSalesGenerator(seed=1, worker_id=0, run_id="run-fallback")
    loop = IngestLoop(
        channel=channel,
        generator=generator,
        row_bytes=200,
        max_batch_rows=1000,
        target_batch_bytes=1_048_576,
        max_in_flight=4,
        floor_rows=10,
        target_bps_fn=lambda: 1_000_000,
        use_fast_append=True,
    )

    rows, batch_bytes, _sleep_for = loop.run_once()
    assert rows > 0
    assert batch_bytes == rows * 200
    loop.flush()
