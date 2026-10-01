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

"""Streamlit dashboard for elastic channel ramp ingest monitoring."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import (
    col,
    count,
    lit,
)
from snowflake.snowpark.functions import (
    max as sf_max,
)
from snowflake.snowpark.functions import (
    min as sf_min,
)

import streamlit as st

st.set_page_config(page_title="Elastic Channel Ramp Monitor", layout="wide")

REFRESH_OPTIONS: dict[str, int | None] = {
    "Off": None,
    "30 seconds": 30,
    "1 minute": 60,
    "2 minutes": 120,
}
LIVE_RUN_THRESHOLD_SECONDS = 120
RECENT_WINDOW_MINUTES = 10
DEFAULT_WINDOW_MINUTES = 10
QUERY_TIMEOUT_SECONDS = 30
# METADATA$ROW_LAST_COMMIT_TIME requires per-row metadata reads; sample at scale.
LATENCY_SAMPLE_BERNOULLI_PCT = float(
    os.environ.get("ELASTIC_RAMP_LATENCY_SAMPLE_PCT", "1")
)
LATENCY_MAX_WINDOW_MINUTES = int(
    os.environ.get("ELASTIC_RAMP_LATENCY_MAX_WINDOW_MINUTES", "10")
)
LATENCY_RECENT_COMMIT_MINUTES = int(
    os.environ.get("ELASTIC_RAMP_LATENCY_RECENT_COMMIT_MINUTES", "10")
)
# Billion-row RESORT_SALES scans need commit-time pruning and capped windows on LARGE_WH+.
QUERY_MAX_WINDOW_MINUTES = int(
    os.environ.get("ELASTIC_RAMP_QUERY_MAX_WINDOW_MINUTES", "10")
)
WORKER_MAX_WINDOW_MINUTES = int(
    os.environ.get("ELASTIC_RAMP_WORKER_MAX_WINDOW_MINUTES", "5")
)
ROW_BYTES_ESTIMATE = 217
BYTES_PER_MB = 1_000_000
# INGEST_TS reflects Snowpipe flush time (often a few coarse buckets).
# SALE_TIME is stamped per client batch during the ramp and tracks send rate.
CLIENT_TIME_COLUMN = os.environ.get("ELASTIC_RAMP_CLIENT_TIME_COLUMN", "SALE_TIME")
ROW_COMMIT_COLUMN = "METADATA$ROW_LAST_COMMIT_TIME"
TIME_COLUMN = CLIENT_TIME_COLUMN


def _table_fqn() -> str:
    db = os.environ.get("SNOWFLAKE_DATABASE", "SC_DB")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "SC_SCHEMA")
    table = os.environ.get("ELASTIC_TABLE_NAME", "RESORT_SALES")
    return f"{db}.{schema}.{table}"


def _find_col(df: pd.DataFrame, name: str) -> str:
    target = name.upper()
    for column in df.columns:
        if str(column).upper() == target:
            return str(column)
    raise KeyError(name)


def _as_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0).astype("float64")


def _to_utc(value: object) -> pd.Timestamp:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return pd.Timestamp.now(tz="UTC")
    return parsed


def _window_anchor(summary: pd.DataFrame) -> pd.Timestamp:
    """Anchor charts to run end time, or now when the run is still ingesting."""
    last = _to_utc(summary.iloc[0][_find_col(summary, "last_event")])
    now = pd.Timestamp.now(tz="UTC")
    if (now - last).total_seconds() <= LIVE_RUN_THRESHOLD_SECONDS:
        return now
    return last


def _window_bounds(
    summary: pd.DataFrame, window_minutes: int
) -> tuple[pd.Timestamp, pd.Timestamp]:
    end = _window_anchor(summary)
    first = _to_utc(summary.iloc[0][_find_col(summary, "first_event")])
    start = end - pd.Timedelta(minutes=window_minutes)
    start = max(start, first)
    return start, end


def _capped_window_bounds(
    summary: pd.DataFrame,
    window_minutes: int,
    cap_minutes: int,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Cap aggregation scans — RESORT_SALES can hold tens of billions of rows per run."""
    chart_start, chart_end = _window_bounds(summary, window_minutes)
    capped_minutes = min(window_minutes, cap_minutes)
    end = chart_end
    start = end - pd.Timedelta(minutes=capped_minutes)
    first = _to_utc(summary.iloc[0][_find_col(summary, "first_event")])
    start = max(start, first)
    start = max(start, chart_start)
    return start, end


def _latency_window_bounds(
    summary: pd.DataFrame, window_minutes: int
) -> tuple[pd.Timestamp, pd.Timestamp]:
    return _capped_window_bounds(summary, window_minutes, LATENCY_MAX_WINDOW_MINUTES)


def _throughput_window_bounds(
    summary: pd.DataFrame, window_minutes: int
) -> tuple[pd.Timestamp, pd.Timestamp]:
    return _capped_window_bounds(summary, window_minutes, QUERY_MAX_WINDOW_MINUTES)


def _worker_window_bounds(
    summary: pd.DataFrame, window_minutes: int
) -> tuple[pd.Timestamp, pd.Timestamp]:
    return _capped_window_bounds(summary, window_minutes, WORKER_MAX_WINDOW_MINUTES)


def _recent_window_bounds(summary: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Last N minutes anchored to now (live) or the run's last event."""
    end = _window_anchor(summary)
    first = _to_utc(summary.iloc[0][_find_col(summary, "first_event")])
    start = end - pd.Timedelta(minutes=RECENT_WINDOW_MINUTES)
    start = max(start, first)
    return start, end


def _ensure_recent_minute_grid(
    pdf: pd.DataFrame,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> pd.DataFrame:
    """Fill missing minutes with zero so the recent-window chart has full buckets."""
    end_min = _to_utc(window_end).floor("min")
    start_min = end_min - pd.Timedelta(minutes=RECENT_WINDOW_MINUTES - 1)
    window_start_min = _to_utc(window_start).floor("min")
    start_min = max(start_min, window_start_min)

    minutes = pd.date_range(start_min, end_min, freq="min", tz="UTC")
    grid = pd.DataFrame({"minute_bucket": minutes.strftime("%Y-%m-%d %H:%M")})

    if pdf.empty:
        grid["row_count"] = 0.0
        grid["rows_per_second"] = 0.0
        grid["bytes_per_second"] = 0.0
        grid["mb_per_second"] = 0.0
        return grid

    bucket = _find_col(pdf, "minute_bucket")
    data = pdf[
        [bucket, "row_count", "rows_per_second", "bytes_per_second", "mb_per_second"]
    ].copy()
    data = data.rename(columns={bucket: "minute_bucket"})

    merged = grid.merge(data, on="minute_bucket", how="left")
    merged["row_count"] = _as_float(merged["row_count"]).fillna(0.0)
    merged["rows_per_second"] = _as_float(merged["rows_per_second"]).fillna(0.0)
    merged["bytes_per_second"] = _as_float(merged["bytes_per_second"]).fillna(0.0)
    merged["mb_per_second"] = _as_float(merged["mb_per_second"]).fillna(0.0)
    return merged


def _load_recent_minutes(
    session,
    run_id: str,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> pd.DataFrame:
    pdf = _load_throughput(session, run_id, window_start, window_end)
    return _ensure_recent_minute_grid(pdf, window_start, window_end)


def _configure_session(session) -> None:
    session.sql(
        f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {QUERY_TIMEOUT_SECONDS}"
    ).collect()


def _init_widgets() -> tuple[str | None, int, int | None, int]:
    if "elastic_ramp_run_id" not in st.session_state:
        st.session_state.elastic_ramp_run_id = str(
            os.environ.get("ELASTIC_RAMP_RUN_ID", "") or ""
        )
    if "elastic_ramp_window" not in st.session_state:
        st.session_state.elastic_ramp_window = DEFAULT_WINDOW_MINUTES
    if "elastic_ramp_refresh" not in st.session_state:
        st.session_state.elastic_ramp_refresh = "Off"
    if "elastic_ramp_worker_limit" not in st.session_state:
        st.session_state.elastic_ramp_worker_limit = 16

    run_id = st.sidebar.text_input("Run ID filter", key="elastic_ramp_run_id")
    run_id = str(run_id or "").strip() or None
    window = int(
        st.sidebar.slider(
            "Window (minutes)",
            min_value=5,
            max_value=120,
            key="elastic_ramp_window",
        )
    )
    worker_limit = int(
        st.sidebar.slider(
            "Workers shown in chart",
            min_value=4,
            max_value=96,
            key="elastic_ramp_worker_limit",
        )
    )
    refresh_label = st.sidebar.selectbox(
        "Auto-refresh",
        options=list(REFRESH_OPTIONS.keys()),
        key="elastic_ramp_refresh",
    )
    refresh_seconds = REFRESH_OPTIONS[refresh_label]
    return run_id, window, refresh_seconds, worker_limit


def _resolve_run_id(session, run_id: str | None) -> str | None:
    if run_id:
        return run_id
    table = _table_fqn()
    query = f"""
        SELECT RUN_ID
        FROM {table}
        WHERE {ROW_COMMIT_COLUMN} >= DATEADD(
            'hour', -24, CURRENT_TIMESTAMP()
        )
        GROUP BY RUN_ID
        ORDER BY MAX({TIME_COLUMN}) DESC
        LIMIT 1
    """
    rows = session.sql(query).collect()
    if not rows:
        return None
    return str(rows[0]["RUN_ID"])


def _load_summary(session, run_id: str) -> pd.DataFrame:
    summary = (
        session.table(_table_fqn())
        .filter(col("RUN_ID") == lit(run_id))
        .group_by(col("RUN_ID"))
        .agg(
            count("*").alias("total_rows"),
            sf_min(col(TIME_COLUMN)).alias("first_event"),
            sf_max(col(TIME_COLUMN)).alias("last_event"),
            sf_min(col("INGEST_TS")).alias("first_ingest"),
            sf_max(col("INGEST_TS")).alias("last_ingest"),
            count(col("WORKER_ID")).alias("rows_with_worker_id"),
        )
        .to_pandas()
    )
    if summary.empty:
        return summary
    rows_col = _find_col(summary, "total_rows")
    summary[rows_col] = _as_float(summary[rows_col])
    return summary


def _window_sql_params(
    window_start: pd.Timestamp, window_end: pd.Timestamp
) -> tuple[str, str, str, str]:
    start_ntz = _sql_timestamp_ntz(window_start)
    end_ntz = _sql_timestamp_ntz(window_end)
    start_utc = _sql_timestamp_tz_utc(window_start)
    end_utc = _sql_timestamp_tz_utc(window_end)
    return start_ntz, end_ntz, start_utc, end_utc


def _load_throughput(
    session,
    run_id: str,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> pd.DataFrame:
    table = _table_fqn()
    start_ntz, end_ntz, start_utc, end_utc = _window_sql_params(
        window_start, window_end
    )
    query = f"""
        SELECT
            DATE_TRUNC('minute', {TIME_COLUMN}) AS minute_bucket,
            COUNT(*) AS row_count
        FROM {table}
        WHERE RUN_ID = ?
          AND {TIME_COLUMN} >= TO_TIMESTAMP_NTZ(?)
          AND {TIME_COLUMN} <= TO_TIMESTAMP_NTZ(?)
          AND {ROW_COMMIT_COLUMN} >= TO_TIMESTAMP_TZ(? || ' +0000')
          AND {ROW_COMMIT_COLUMN} <= TO_TIMESTAMP_TZ(? || ' +0000')
        GROUP BY 1
        ORDER BY 1
    """
    pdf = session.sql(
        query,
        params=[run_id, start_ntz, end_ntz, start_utc, end_utc],
    ).to_pandas()
    if pdf.empty:
        return pdf

    bucket = _find_col(pdf, "minute_bucket")
    count_col = _find_col(pdf, "row_count")
    pdf[bucket] = (
        pd.to_datetime(pdf[bucket], utc=True, errors="coerce")
        .dt.strftime("%Y-%m-%d %H:%M")
    )
    pdf["row_count"] = _as_float(pdf[count_col])
    pdf["rows_per_second"] = pdf["row_count"] / 60.0
    pdf["bytes_per_second"] = pdf["row_count"] * ROW_BYTES_ESTIMATE / 60.0
    pdf["mb_per_second"] = pdf["bytes_per_second"] / BYTES_PER_MB
    return pdf


def _worker_label(value: object) -> str:
    try:
        return f"w{int(value)}"
    except (TypeError, ValueError):
        return "w?"


def _load_worker_rates(
    session,
    run_id: str,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> pd.DataFrame:
    table = _table_fqn()
    start_ntz, end_ntz, start_utc, end_utc = _window_sql_params(
        window_start, window_end
    )
    query = f"""
        SELECT
            WORKER_ID,
            DATE_TRUNC('minute', {TIME_COLUMN}) AS minute_bucket,
            COUNT(*) AS rows_per_minute
        FROM {table}
        WHERE RUN_ID = ?
          AND WORKER_ID IS NOT NULL
          AND {TIME_COLUMN} >= TO_TIMESTAMP_NTZ(?)
          AND {TIME_COLUMN} <= TO_TIMESTAMP_NTZ(?)
          AND {ROW_COMMIT_COLUMN} >= TO_TIMESTAMP_TZ(? || ' +0000')
          AND {ROW_COMMIT_COLUMN} <= TO_TIMESTAMP_TZ(? || ' +0000')
        GROUP BY 1, 2
        ORDER BY 2, 1
    """
    pdf = session.sql(
        query,
        params=[run_id, start_ntz, end_ntz, start_utc, end_utc],
    ).to_pandas()
    if pdf.empty:
        return pdf

    bucket = _find_col(pdf, "minute_bucket")
    worker = _find_col(pdf, "worker_id")
    rate = _find_col(pdf, "rows_per_minute")
    pdf[bucket] = (
        pd.to_datetime(pdf[bucket], utc=True, errors="coerce")
        .dt.strftime("%Y-%m-%d %H:%M")
    )
    pdf[worker] = pdf[worker].apply(_worker_label)
    pdf[rate] = _as_float(pdf[rate])
    return pdf


def _sql_timestamp_ntz(ts: pd.Timestamp) -> str:
    """UTC wall clock for TIMESTAMP_NTZ bind parameters."""
    return _to_utc(ts).tz_convert("UTC").tz_localize(None).strftime("%Y-%m-%d %H:%M:%S")


def _sql_timestamp_tz_utc(ts: pd.Timestamp) -> str:
    """UTC instant for TIMESTAMP_TZ bind parameters (append +0000)."""
    return _to_utc(ts).strftime("%Y-%m-%d %H:%M:%S")


def _client_timestamp_expr(column: str) -> str:
    """SALE_TIME is UTC wall-clock NTZ from the ingest client."""
    return f"TO_TIMESTAMP_TZ({column} || ' +0000')"


def _latency_sample_pct() -> float:
    pct = LATENCY_SAMPLE_BERNOULLI_PCT
    if pct <= 0:
        return 1.0
    return min(pct, 100.0)


def _hash_sample_predicate(sample_pct: float) -> str:
    """Deterministic row sample applied after RUN_ID/time filters."""
    if sample_pct >= 100:
        return "TRUE"
    buckets = max(1, round(100 / sample_pct))
    return f"MOD(ABS(HASH(TXID)), {buckets}) = 0"


def _load_ingest_latency(
    session,
    run_id: str,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> pd.DataFrame:
    """Avg ingest latency by client-time minute using METADATA$ROW_LAST_COMMIT_TIME.

    Commit-time bounds prune ROW_TIMESTAMP micro-partitions; hash sampling avoids
    reading metadata for every row in high-throughput windows.

    See https://docs.snowflake.com/en/user-guide/data-engineering/row-timestamps
    """
    table = _table_fqn()
    client_ts = _client_timestamp_expr(CLIENT_TIME_COLUMN)
    start_ntz = _sql_timestamp_ntz(window_start)
    end_ntz = _sql_timestamp_ntz(window_end)
    start_utc = _sql_timestamp_tz_utc(window_start)
    end_utc = _sql_timestamp_tz_utc(window_end)
    sample_pct = _latency_sample_pct()
    sample_predicate = _hash_sample_predicate(sample_pct)
    query = f"""
        SELECT
            DATE_TRUNC('minute', {CLIENT_TIME_COLUMN}) AS minute_bucket,
            AVG(
                TIMESTAMPDIFF(
                    'millisecond',
                    {client_ts},
                    {ROW_COMMIT_COLUMN}
                )
            ) AS avg_latency_ms,
            COUNT(*) AS sample_rows
        FROM {table}
        WHERE RUN_ID = ?
          AND {CLIENT_TIME_COLUMN} >= TO_TIMESTAMP_NTZ(?)
          AND {CLIENT_TIME_COLUMN} <= TO_TIMESTAMP_NTZ(?)
          AND {ROW_COMMIT_COLUMN} >= TO_TIMESTAMP_TZ(? || ' +0000')
          AND {ROW_COMMIT_COLUMN} <= TO_TIMESTAMP_TZ(? || ' +0000')
          AND {ROW_COMMIT_COLUMN} IS NOT NULL
          AND {sample_predicate}
        GROUP BY 1
        HAVING avg_latency_ms IS NOT NULL
        ORDER BY 1
    """
    pdf = session.sql(
        query,
        params=[run_id, start_ntz, end_ntz, start_utc, end_utc],
    ).to_pandas()
    if pdf.empty:
        return pdf

    bucket = _find_col(pdf, "minute_bucket")
    latency_col = _find_col(pdf, "avg_latency_ms")
    pdf[bucket] = (
        pd.to_datetime(pdf[bucket], utc=True, errors="coerce")
        .dt.strftime("%Y-%m-%d %H:%M")
    )
    pdf["avg_latency_ms"] = _as_float(pdf[latency_col])
    pdf["avg_latency_sec"] = pdf["avg_latency_ms"] / 1000.0
    pdf["avg_latency_ms"] = pdf["avg_latency_ms"].clip(lower=0.0)
    pdf["avg_latency_sec"] = pdf["avg_latency_sec"].clip(lower=0.0)
    return pdf


def _line_chart(
    df: pd.DataFrame, x_col: str, y_col: str, *, y_label: str | None = None
) -> None:
    chart = df[[x_col, y_col]].copy()
    chart[x_col] = chart[x_col].astype(str)
    chart[y_col] = chart[y_col].astype("float64")
    if y_label:
        chart = chart.rename(columns={y_col: y_label})
    st.line_chart(chart.set_index(x_col), use_container_width=True)


def _bar_chart(df: pd.DataFrame, x_col: str, y_col: str) -> None:
    chart = df[[x_col, y_col]].copy()
    chart[x_col] = chart[x_col].astype(str)
    chart[y_col] = chart[y_col].astype("float64")
    st.bar_chart(chart.set_index(x_col), use_container_width=True)


def _worker_line_chart(df: pd.DataFrame, worker_limit: int) -> None:
    bucket = _find_col(df, "minute_bucket")
    worker = _find_col(df, "worker_id")
    rate = _find_col(df, "rows_per_minute")

    totals = (
        df.groupby(worker, as_index=False)[rate]
        .sum()
        .sort_values(rate, ascending=False)
    )
    keep = set(totals.head(worker_limit)[worker].astype(str))
    filtered = df[df[worker].astype(str).isin(keep)].copy()

    pivot = (
        filtered.pivot_table(index=bucket, columns=worker, values=rate, aggfunc="sum")
        .fillna(0.0)
        .astype("float64")
        .sort_index()
    )
    pivot.index = pivot.index.astype(str)
    pivot.columns = pivot.columns.astype(str)
    st.line_chart(pivot, use_container_width=True)
    if len(totals) > worker_limit:
        st.caption(
            f"Showing top {worker_limit} workers by row volume "
            f"({len(totals)} workers total). Increase slider to show more."
        )


def _format_mb_per_sec(mbps: float) -> str:
    mbps = float(mbps)
    if mbps >= 1000:
        return f"{mbps / 1000:.2f} GB/s"
    if mbps >= 1:
        return f"{mbps:.2f} MB/s"
    if mbps >= 0.001:
        return f"{mbps * 1000:.2f} KB/s"
    return f"{mbps * 1_000_000:.0f} B/s"


def _format_duration(seconds: float) -> str:
    seconds = max(0.0, float(seconds))
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    if seconds < 60:
        return f"{seconds:.1f} s"
    if seconds < 3600:
        minutes, secs = divmod(int(seconds), 60)
        return f"{minutes}m {secs}s"
    hours, rem = divmod(int(seconds), 3600)
    minutes = rem // 60
    return f"{hours}h {minutes}m"


def _load_latest_row_latency_ms(session, run_id: str) -> float | None:
    """Latency for the most recently committed row: commit time − client row time."""
    table = _table_fqn()
    client_ts = _client_timestamp_expr(CLIENT_TIME_COLUMN)
    query = f"""
        SELECT
            TIMESTAMPDIFF(
                'millisecond',
                {client_ts},
                {ROW_COMMIT_COLUMN}
            ) AS latency_ms
        FROM {table}
        WHERE RUN_ID = ?
          AND {ROW_COMMIT_COLUMN} IS NOT NULL
          AND {ROW_COMMIT_COLUMN} >= DATEADD(
              'minute', -{LATENCY_RECENT_COMMIT_MINUTES}, CURRENT_TIMESTAMP()
          )
        ORDER BY {ROW_COMMIT_COLUMN} DESC
        LIMIT 1
    """
    rows = session.sql(query, params=[run_id]).collect()
    if not rows:
        return None
    ms = rows[0]["LATENCY_MS"]
    if ms is None:
        return None
    return max(0.0, float(ms))


def _format_ts(ts: pd.Timestamp) -> str:
    return _to_utc(ts).strftime("%Y-%m-%d %H:%M:%S UTC")


def main() -> None:
    st.title("Elastic Channel Ramp Monitor")
    st.caption(
        "Throughput bucketed on SALE_TIME (client send time). Current latency is "
        f"{ROW_COMMIT_COLUMN} minus {CLIENT_TIME_COLUMN} for the latest committed "
        "row; charts average the same diff per minute "
        "([row timestamps](https://docs.snowflake.com/en/user-guide/data-engineering/row-timestamps))."
    )

    run_id, window, refresh_seconds, worker_limit = _init_widgets()

    try:
        session = get_active_session()
        _configure_session(session)
        run_id = _resolve_run_id(session, run_id)
        if not run_id:
            st.warning("No ingested rows found in RESORT_SALES.")
            return

        summary = _load_summary(session, run_id)
        if summary.empty:
            st.warning(f"No rows found for run_id={run_id}.")
            return

        window_start, window_end = _window_bounds(summary, window)
        throughput_start, throughput_end = _throughput_window_bounds(summary, window)
        worker_start, worker_end = _worker_window_bounds(summary, window)
        latency_start, latency_end = _latency_window_bounds(summary, window)
        recent_start, recent_end = _recent_window_bounds(summary)

        throughput = pd.DataFrame()
        worker_rates = pd.DataFrame()
        recent_minutes = pd.DataFrame()
        throughput_error: str | None = None
        worker_error: str | None = None
        try:
            recent_minutes = _load_recent_minutes(
                session, run_id, recent_start, recent_end
            )
            throughput = _load_throughput(
                session, run_id, throughput_start, throughput_end
            )
        except Exception as throughput_exc:
            throughput_error = str(throughput_exc)
        try:
            worker_rates = _load_worker_rates(
                session, run_id, worker_start, worker_end
            )
        except Exception as worker_exc:
            worker_error = str(worker_exc)

        latest_row_latency_ms: float | None = None
        ingest_latency = pd.DataFrame()
        latency_error: str | None = None
        try:
            latest_row_latency_ms = _load_latest_row_latency_ms(session, run_id)
            ingest_latency = _load_ingest_latency(
                session, run_id, latency_start, latency_end
            )
        except Exception as latency_exc:
            latency_error = str(latency_exc)
    except Exception as exc:
        st.error(f"Could not query Snowflake: {exc}")
        st.info("Ensure the Streamlit app role can SELECT from RESORT_SALES.")
        return

    last_event = _to_utc(summary.iloc[0][_find_col(summary, "last_event")])
    is_live = (
        pd.Timestamp.now(tz="UTC") - last_event
    ).total_seconds() <= LIVE_RUN_THRESHOLD_SECONDS
    status = "live" if is_live else "completed run"
    bucket_count = len(throughput) if not throughput.empty else 0
    st.caption(
        f"Chart window ({status}, {bucket_count} buckets queried over "
        f"{_format_ts(throughput_start)} → {_format_ts(throughput_end)}, "
        f"max {QUERY_MAX_WINDOW_MINUTES} min): "
        f"slider {_format_ts(window_start)} → {_format_ts(window_end)}"
    )

    total_rows = int(_as_float(summary[_find_col(summary, "total_rows")]).sum())
    metric_a, metric_b, metric_c, metric_d = st.columns(4)
    metric_a.metric("Total rows", f"{total_rows:,}")
    metric_a.caption(f"Run ID: {run_id}")

    if not throughput.empty:
        latest_rps = float(throughput.iloc[-1]["rows_per_second"])
        latest_mbps = float(throughput.iloc[-1]["mb_per_second"])
        metric_b.metric("Latest rows/sec", f"{latest_rps:,.1f}")
        metric_c.metric("Est. throughput", _format_mb_per_sec(latest_mbps))
    else:
        metric_b.metric("Latest rows/sec", "n/a")
        metric_c.metric("Est. throughput", "n/a")

    if latest_row_latency_ms is not None:
        latest_row_latency_sec = latest_row_latency_ms / 1000.0
        metric_d.metric("Current latency", _format_duration(latest_row_latency_sec))
        caption = (
            f"{ROW_COMMIT_COLUMN} − {CLIENT_TIME_COLUMN} "
            f"(latest committed row)"
        )
        if not ingest_latency.empty:
            latest_bucket_ms = float(ingest_latency.iloc[-1]["avg_latency_ms"])
            caption += (
                f" · avg {_format_duration(latest_bucket_ms / 1000.0)} "
                f"(latest {CLIENT_TIME_COLUMN} minute)"
            )
        metric_d.caption(caption)
    else:
        metric_d.metric("Current latency", "n/a")
        metric_d.caption(
            f"{ROW_COMMIT_COLUMN} − {CLIENT_TIME_COLUMN} n/a "
            f"(enable ROW_TIMESTAMP on RESORT_SALES; rows before enable are NULL)"
        )

    st.subheader(f"Last {RECENT_WINDOW_MINUTES} minutes")
    st.caption(
        f"Records and avg throughput per minute ({_format_ts(recent_start)} → "
        f"{_format_ts(recent_end)}), bucketed on {CLIENT_TIME_COLUMN}."
    )
    if recent_minutes["row_count"].sum() == 0:
        st.info(f"No rows in the last {RECENT_WINDOW_MINUTES} minutes for this run.")
    else:
        _bar_chart(recent_minutes, "minute_bucket", "row_count")
        _line_chart(recent_minutes, "minute_bucket", "mb_per_second", y_label="MB/s")
        avg_rps = float(recent_minutes["rows_per_second"].mean())
        avg_mbps = float(recent_minutes["mb_per_second"].mean())
        st.caption(
            f"{RECENT_WINDOW_MINUTES}-min avg: {int(recent_minutes['row_count'].sum()):,} records · "
            f"{avg_rps:,.0f} rows/s · {_format_mb_per_sec(avg_mbps)}"
        )

    st.subheader("Aggregate throughput (1-minute buckets)")
    if throughput_error:
        st.warning(f"Throughput query failed: {throughput_error}")
    elif throughput.empty:
        st.info(
            "No rows in the selected window. Increase the window (minutes) slider "
            f"or confirm {TIME_COLUMN} is populated for this run."
        )
    else:
        bucket = _find_col(throughput, "minute_bucket")
        st.caption(
            "Throughput estimated from row count × ~217 bytes, shown as MB/s (decimal). "
            f"Queries cap at {QUERY_MAX_WINDOW_MINUTES} min and filter "
            f"{ROW_COMMIT_COLUMN} to prune micro-partitions."
        )
        _line_chart(throughput, bucket, "mb_per_second", y_label="MB/s")

    st.subheader("Average ingest latency (1-minute buckets)")
    sample_pct = _latency_sample_pct()
    st.caption(
        f"TIMESTAMPDIFF({CLIENT_TIME_COLUMN} → {ROW_COMMIT_COLUMN}), averaged per "
        f"{CLIENT_TIME_COLUMN} minute over {_format_ts(latency_start)} → "
        f"{_format_ts(latency_end)} (max {LATENCY_MAX_WINDOW_MINUTES} min). "
        f"~{sample_pct:g}% hash sample on TXID — full scans of row commit metadata "
        "timeout on multi-billion-row tables. Requires ROW_TIMESTAMP = TRUE on RESORT_SALES."
    )
    if latency_error:
        st.warning(f"Latency query failed: {latency_error}")
    elif ingest_latency.empty:
        st.info(
            "No latency buckets in this window. Confirm ROW_TIMESTAMP is enabled, "
            "ingest is active for this run_id, and the window covers recent "
            f"{CLIENT_TIME_COLUMN} minutes."
        )
    else:
        bucket = _find_col(ingest_latency, "minute_bucket")
        _line_chart(ingest_latency, bucket, "avg_latency_sec", y_label="Latency (s)")
        sample_col = _find_col(ingest_latency, "sample_rows")
        overall_ms = (
            ingest_latency["avg_latency_ms"] * ingest_latency[sample_col]
        ).sum() / max(_as_float(ingest_latency[sample_col]).sum(), 1.0)
        st.caption(
            f"Samples: {int(_as_float(ingest_latency[sample_col]).sum()):,} rows · "
            f"window avg {_format_duration(overall_ms / 1000.0)}"
        )

    st.subheader("Per-worker message rate")
    if worker_error:
        st.warning(f"Worker rate query failed: {worker_error}")
    elif worker_rates.empty:
        st.info(
            "No per-worker data in the selected window. Confirm WORKER_ID is populated "
            "and widen the window slider."
        )
    else:
        _worker_line_chart(worker_rates, worker_limit)
        st.caption(
            f"Worker chart uses the last {WORKER_MAX_WINDOW_MINUTES} minutes max "
            f"({_format_ts(worker_start)} → {_format_ts(worker_end)})."
        )

    with st.expander("Raw summary"):
        st.dataframe(summary)

    updated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if refresh_seconds:
        st.caption(f"Last updated: {updated} · auto-refresh every {refresh_seconds}s")
        time.sleep(refresh_seconds)
        st.rerun()
    else:
        st.caption(f"Last updated: {updated}")


main()
