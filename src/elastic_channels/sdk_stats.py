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

"""Sample the ingest SDK's own Prometheus metrics (compressed vs uncompressed bytes).

The `snowpipe-streaming` SDK has no Python-level stats getter; it only exposes an
internal Rust telemetry recorder via an optional Prometheus HTTP endpoint
(`SS_ENABLE_METRICS=true`, must be set before the SDK module is imported/bootstrapped).

Each worker pod runs its own process with its own loopback Prometheus exporter port
(`ELASTIC_SDK_METRICS_PORT`, default 9090 — no clash, since each worker is a separate
pod/network namespace); `k8s_worker.py` scrapes only its own endpoint each interval
and computes its own delta via `compute_delta()` below. Cross-worker aggregation
happens downstream in `k8s_monitor.py`, from each worker's already-computed
per-interval numbers (stdout JSON), not by scraping every worker's `/metrics`
endpoint from one central place.

**Why compressed-bytes tracking sums two counters:** the SDK routes each append
batch through one of two code paths depending on backpressure — inline "rowset"
mode (compressed HTTP POST body, `output_payload_compressed_bytes`) or
"file-fragment" mode (upload to cloud storage, `output_file_compressed_bytes`) —
and only the active path's counter advances. A channel sustaining high throughput
spends most of its time in file-fragment mode, so `output_payload_compressed_bytes`
stops advancing and is eventually evicted from `/metrics` entirely by the exporter's
idle-timeout. Treating a missing counter as a real `0.0` would manufacture a fake
throughput collapse. This module therefore sums both counters' deltas and only
marks compression data unavailable when **both** are missing for an interval.
`input_payload_bytes` (uncompressed) is unaffected by this — it's recorded at
append time, before the mode decision is made.
"""

from __future__ import annotations

import os
import re
import urllib.error
import urllib.request
from dataclasses import dataclass

_METRIC_LINE_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([^\s]+)\s*$")

# Prometheus summary `_sum` counters are monotonically increasing totals; deltas
# between samples give bytes transferred in the interval.
UNCOMPRESSED_BYTES_METRIC = "input_payload_bytes_sum"
# Rowset (inline HTTP) mode compressed-bytes counter — only updated while the
# channel's ModeSelector is choosing FlushMode::Inline. See module docstring.
COMPRESSED_BYTES_METRIC = "output_payload_compressed_bytes_sum"
# File-fragment (cloud storage upload) mode's counterpart — only updated while the
# channel is in FlushMode::FileFragment. Summed with COMPRESSED_BYTES_METRIC below
# to get the true compressed total regardless of which mode was active.
FILE_COMPRESSED_BYTES_METRIC = "output_file_compressed_bytes_sum"
BUFFER_LIMIT_REACHED_METRIC = "input_buffer_byte_limit_reached_count"
RETRYABLE_ERROR_METRIC = "sf_api_retryable_error_count"
INPUT_BUFFER_BYTES_METRIC = "input_buffer_bytes"


def parse_prometheus_text(body: str) -> dict[str, float]:
    """Parse Prometheus exposition text into {metric_name: value}.

    Multiple label-series for the same metric name are summed (this project runs one
    elastic channel per client, so in practice there is exactly one series per name).
    """
    values: dict[str, float] = {}
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = _METRIC_LINE_RE.match(line)
        if not match:
            continue
        name, _labels, raw_value = match.groups()
        try:
            value = float(raw_value)
        except ValueError:
            continue
        values[name] = values.get(name, 0.0) + value
    return values


def fetch_sdk_metrics(url: str, *, timeout: float = 2.0) -> dict[str, float] | None:
    """Scrape a worker's `/metrics` endpoint; return None if unreachable."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError, TimeoutError):
        return None
    return parse_prometheus_text(body)


@dataclass(frozen=True)
class SdkStatsDelta:
    uncompressed_bps: float
    compressed_bps: float
    compression_ratio: float
    input_buffer_bytes: float
    buffer_limit_reached_total: int
    retryable_errors_total: int
    # False when `output_payload_compressed_bytes` was evicted by the SDK's exporter
    # this interval (see module docstring): `compressed_bps`/`compression_ratio` are
    # then meaningless zeros, not a measured drop to zero. `uncompressed_bps` is still
    # valid — `input_payload_bytes` is a channel-level counter that stays present.
    compression_metrics_available: bool = True


def _counter_delta(prev: dict[str, float], curr: dict[str, float], key: str) -> float | None:
    """Delta of a single monotonic counter between two scrapes.

    Returns None if `key` is missing from either scrape (not present, or evicted by
    the SDK's exporter — see module docstring) or the value went backwards (process
    restart). Missing is deliberately distinct from `0.0`.
    """
    if key not in prev or key not in curr:
        return None
    prev_val = prev[key]
    curr_val = curr[key]
    if curr_val < prev_val:
        return None
    return curr_val - prev_val


def compute_delta(
    prev: dict[str, float], curr: dict[str, float], elapsed_seconds: float
) -> SdkStatsDelta | None:
    """Compute one worker's bytes/sec deltas between two scrapes.

    Returns None if `elapsed_seconds` is non-positive, or the *uncompressed* byte
    counter (`input_payload_bytes`) is missing from either scrape or went backwards
    (the worker process restarted mid-sample with a fresh client) — that counter is
    recorded at append time regardless of inline/file-fragment routing, so its
    absence means the worker itself didn't report, not a routing artifact.

    The *compressed* byte total sums two independent counters — rowset-mode
    (`output_payload_compressed_bytes`) and file-fragment-mode
    (`output_file_compressed_bytes`) — because the SDK routes each batch through
    exactly one of those two code paths depending on backpressure (see module
    docstring), and only the active path's counter advances. Either may be
    missing/evicted for an interval where the channel favored the other mode;
    `compression_metrics_available` is only False when **both** are missing, so a
    channel that's, say, fully in file-fragment mode still reports its real
    compressed throughput instead of a manufactured zero.
    """
    if elapsed_seconds <= 0:
        return None
    uncomp_delta = _counter_delta(prev, curr, UNCOMPRESSED_BYTES_METRIC)
    if uncomp_delta is None:
        return None

    rowset_comp_delta = _counter_delta(prev, curr, COMPRESSED_BYTES_METRIC)
    file_comp_delta = _counter_delta(prev, curr, FILE_COMPRESSED_BYTES_METRIC)
    compression_available = rowset_comp_delta is not None or file_comp_delta is not None
    comp_delta = (rowset_comp_delta or 0.0) + (file_comp_delta or 0.0)
    comp_bps = comp_delta / elapsed_seconds if compression_available else 0.0
    ratio = uncomp_delta / comp_delta if comp_delta > 0 else 0.0

    return SdkStatsDelta(
        uncompressed_bps=uncomp_delta / elapsed_seconds,
        compressed_bps=comp_bps,
        compression_ratio=ratio,
        input_buffer_bytes=curr.get(INPUT_BUFFER_BYTES_METRIC, 0.0),
        buffer_limit_reached_total=int(curr.get(BUFFER_LIMIT_REACHED_METRIC, 0.0)),
        retryable_errors_total=int(curr.get(RETRYABLE_ERROR_METRIC, 0.0)),
        compression_metrics_available=compression_available,
    )


def maybe_enable_metrics_server(port: int | None) -> None:
    """Set env vars to enable the SDK's Prometheus exporter (call before SDK import)."""
    if port is None:
        return
    os.environ["SS_ENABLE_METRICS"] = "true"
    os.environ["SS_METRICS_PORT"] = str(port)
    os.environ.setdefault("SS_METRICS_IP", "127.0.0.1")


__all__ = [
    "SdkStatsDelta",
    "compute_delta",
    "fetch_sdk_metrics",
    "maybe_enable_metrics_server",
    "parse_prometheus_text",
]
