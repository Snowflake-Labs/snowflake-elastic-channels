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

"""Fast append path for pre-serialized NDJSON into a Snowpipe Streaming elastic
channel — the other half of the optional JSON fast-path (see
`resort_sales.ResortSalesGenerator.next_batch_ndjson`).

`StreamingIngestElasticChannel.append_rows_with_wait` takes a list of row
dicts and, on every call, re-derives the same NDJSON bytes we already built
ourselves: it runs `convert_bytes_in_row` over every row, then
`self._serializer.encode_lines(rows)` (see the installed
`snowflake.ingest.streaming.streaming_ingest_elastic_channel` module). Handing
it dicts just to have it re-encode them throws away the fast encoder's output.

`append_rows_ndjson` below calls the exact same underlying Rust append
(`channel._channel.append_rows(json_bytes, num_rows, future_id, ...)`) and
follows the exact same future/callback registration protocol
(`_future_id_lock`, `_next_future_id`, `_callback.register_future`) that
`StreamingIngestElasticChannel._append_rows_internal` uses internally — so the
returned `Future` behaves identically to one from `append_rows_with_wait` for
`IngestLoop`'s backpressure/ack accounting — but takes bytes we already have,
skipping the redundant re-encode.
"""

from __future__ import annotations

from concurrent.futures import Future
from typing import Any

#: Attributes `StreamingIngestElasticChannel` exposes that this bypass relies
#: on; used to detect whether a channel supports the fast-append path at all.
_REQUIRED_ATTRS = ("_future_id_lock", "_next_future_id", "_callback", "_channel")


def supports_fast_append(channel: Any) -> bool:
    """Whether `channel` exposes the private future/callback protocol this
    module needs (true for `StreamingIngestElasticChannel`)."""
    return all(hasattr(channel, attr) for attr in _REQUIRED_ATTRS)


def append_rows_ndjson(
    channel: Any,
    json_bytes: bytes,
    num_rows: int,
    *,
    append_token: object = None,
) -> Future:
    """Append pre-encoded NDJSON bytes via the elastic channel's FFI, skipping
    the SDK's own dict -> JSON re-encode. Returns a `Future` that resolves the
    same way as `channel.append_rows_with_wait(rows, append_token)`.
    """
    if num_rows <= 0:
        future: Future = Future()
        future.set_result(None)
        return future

    future = Future()
    with channel._future_id_lock:
        future_id = channel._next_future_id
        channel._next_future_id += 1
        duplicate = channel._callback.register_future(future_id, future, append_token)
    if duplicate is not None:
        # Monotonic per-channel ids make this a should-never-happen invariant,
        # matching `_append_rows_internal`'s own handling of the same case.
        channel._channel.invalidate_channel(duplicate.message)
        raise duplicate

    try:
        channel._channel.append_rows(json_bytes, num_rows, future_id, None, None)
    except BaseException:
        # Never enqueued (not ack-eligible), so no ack can arrive; drop the
        # just-registered append to avoid a leak.
        channel._callback.remove_future(future_id)
        raise
    return future
