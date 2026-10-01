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

"""Real-time resort point-of-sale dashboard for the RESORT_SALES table.

A second page alongside the ingest monitor (`../Monitor_App.py`), aimed at
showing *what* is flowing through the elastic-channel pipeline rather than
*how fast* — revenue, mix, and geography across the 42-resort synthetic
portfolio (see `elastic_channels/resort_sales.py`), refreshed on a timer so
it visibly updates while a test is running.

The map layer avoids any topojson/GeoJSON fetch (blocked by Snowsight's
Streamlit CSP outside the Mapbox/Carto allowlist) by drawing continents-free
`sphere` + `graticule` Vega-Lite generators, which are computed client-side
with no network call — see https://vega.github.io/vega-lite/docs/data.html.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session

import streamlit as st

st.set_page_config(page_title="Resort Sales — Real-Time", layout="wide", page_icon="🏔️")

REFRESH_OPTIONS: dict[str, int | None] = {
    "Off": None,
    "10 seconds": 10,
    "30 seconds": 30,
    "1 minute": 60,
}
DEFAULT_REFRESH = "30 seconds"
LIVE_RUN_THRESHOLD_SECONDS = 120
QUERY_TIMEOUT_SECONDS = 30
MAX_WINDOW_MINUTES = int(os.environ.get("ELASTIC_RAMP_QUERY_MAX_WINDOW_MINUTES", "10"))
TIME_COLUMN = "SALE_TIME"
ROW_COMMIT_COLUMN = "METADATA$ROW_LAST_COMMIT_TIME"

CATEGORY_META: dict[str, dict[str, str]] = {
    "SEASON_TICKET": {"label": "Season Tickets", "emoji": "🎫", "color": "#4C78A8"},
    "DAY_TICKET": {"label": "Day Tickets", "emoji": "🎟️", "color": "#54A24B"},
    "FOOD_AND_DRINK": {"label": "Food & Drink", "emoji": "🍔", "color": "#F58518"},
    "MERCHANDISE": {"label": "Merchandise", "emoji": "🧢", "color": "#E45756"},
}
CUSTOMER_TYPE_META: dict[str, dict[str, str]] = {
    "PASS_HOLDER": {"label": "Pass Holder", "emoji": "🪪", "color": "#4C78A8"},
    "DAY_VISITOR": {"label": "Day Visitor", "emoji": "🚶", "color": "#F58518"},
}
PAYMENT_META: dict[str, dict[str, str]] = {
    "CREDIT_CARD": {"label": "Credit Card", "emoji": "💳", "color": "#4C78A8"},
    "PASS_CHARGE": {"label": "Pass Charge", "emoji": "🪪", "color": "#72B7B2"},
    "MOBILE_PAY": {"label": "Mobile Pay", "emoji": "📱", "color": "#54A24B"},
    "CASH": {"label": "Cash", "emoji": "💵", "color": "#F58518"},
}

# Approximate coordinates for the 42 synthetic resort names in
# `elastic_channels/resort_sales.py::RESORT_PROFILES` — illustrative
# placement for the map, not surveyed GPS points.
RESORT_COORDS: dict[str, tuple[float, float]] = {
    "Cedarcrest": (39.21, -105.88),
    "Twinpine Alpine": (49.74, -122.41),
    "Silverpine Ridge": (39.12, -105.61),
    "Granite Hollow": (39.28, -105.42),
    "Summit Meadows": (40.18, -110.92),
    "Otter Brook": (39.19, -106.02),
    "Maple Hollow": (44.21, -72.31),
    "Cloudline Basin": (38.41, -119.41),
    "Birch Run": (43.11, -72.22),
    "Starlight Pass": (39.02, -119.68),
    "Copper Finch": (38.41, -106.41),
    "Ironwood Gap": (38.21, -119.52),
    "Frostpeak": (42.51, -72.41),
    "Blue Ledge": (43.02, -71.61),
    "Eagle Notch": (47.21, -120.51),
    "Pinecroft": (43.71, -70.81),
    "Raven Ridge": (42.02, -73.71),
    "Wildfern": (44.02, -70.91),
    "Glacier Bend": (-36.11, 147.91),
    "Whitewater Falls": (-36.41, 146.81),
    "Ember Lake": (-36.51, 146.71),
    "Alpine Fork": (46.21, 8.11),
    "Highfield": (46.02, 7.02),
    "Seven Pines": (40.21, -78.71),
    "Alder Hills": (44.41, -92.21),
    "Valley Green": (41.21, -80.41),
    "Round Rock": (41.21, -75.11),
    "Mill Creek": (41.02, -81.02),
    "Cider Hollow": (41.11, -81.11),
    "Croft Hill": (42.51, -71.41),
    "Quiet Valley": (38.11, -90.21),
    "Shadow Glen": (40.21, -78.81),
    "Jackpine": (41.31, -75.11),
    "Fern Gap": (40.02, -78.71),
    "Banner Knob": (39.41, -76.91),
    "Fox Creek": (40.11, -83.21),
    "North Fen": (42.21, -83.31),
    "Oak Knoll": (38.21, -85.91),
    "Circle Hill": (39.51, -76.41),
    "Snowfield Creek": (39.11, -93.81),
    "Fox Tail": (39.41, -77.21),
    "Willowmont": (42.21, -87.71),
}


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


def _sql_timestamp_ntz(ts: pd.Timestamp) -> str:
    return _to_utc(ts).tz_convert("UTC").tz_localize(None).strftime("%Y-%m-%d %H:%M:%S")


def _sql_timestamp_tz_utc(ts: pd.Timestamp) -> str:
    return _to_utc(ts).strftime("%Y-%m-%d %H:%M:%S")


def _format_ts(ts: pd.Timestamp) -> str:
    return _to_utc(ts).strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_usd(amount: float) -> str:
    amount = float(amount)
    magnitude = abs(amount)
    if magnitude >= 1_000_000_000_000:
        return f"${amount / 1_000_000_000_000:,.2f}T"
    if magnitude >= 1_000_000_000:
        return f"${amount / 1_000_000_000:,.2f}B"
    if magnitude >= 1_000_000:
        return f"${amount / 1_000_000:,.2f}M"
    if magnitude >= 1_000:
        return f"${amount / 1_000:,.1f}K"
    return f"${amount:,.2f}"


def _configure_session(session) -> None:
    session.sql(
        f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {QUERY_TIMEOUT_SECONDS}"
    ).collect()


def _resolve_run_id(session, run_id: str | None) -> str | None:
    if run_id:
        return run_id
    table = _table_fqn()
    rows = session.sql(
        f"""
        SELECT RUN_ID
        FROM {table}
        WHERE {ROW_COMMIT_COLUMN} >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
        GROUP BY RUN_ID
        ORDER BY MAX({TIME_COLUMN}) DESC
        LIMIT 1
        """
    ).collect()
    if not rows:
        return None
    return str(rows[0]["RUN_ID"])


def _load_run_bounds(session, run_id: str) -> pd.DataFrame:
    query = f"""
        SELECT MIN({TIME_COLUMN}) AS first_event, MAX({TIME_COLUMN}) AS last_event,
               COUNT(*) AS total_txns, SUM(AMOUNT) AS total_revenue,
               AVG(AMOUNT) AS avg_ticket
        FROM {_table_fqn()}
        WHERE RUN_ID = ?
    """
    return session.sql(query, params=[run_id]).to_pandas()


def _window_anchor(last_event: pd.Timestamp) -> pd.Timestamp:
    now = pd.Timestamp.now(tz="UTC")
    if (now - last_event).total_seconds() <= LIVE_RUN_THRESHOLD_SECONDS:
        return now
    return last_event


def _load_category_timeseries(
    session, run_id: str, span_start: pd.Timestamp, span_end: pd.Timestamp
) -> pd.DataFrame:
    """One row per (minute, category) — feeds the trend chart, donut, and delta metric."""
    start_ntz, end_ntz = _sql_timestamp_ntz(span_start), _sql_timestamp_ntz(span_end)
    start_utc, end_utc = _sql_timestamp_tz_utc(span_start), _sql_timestamp_tz_utc(span_end)
    query = f"""
        SELECT
            DATE_TRUNC('minute', {TIME_COLUMN}) AS minute_bucket,
            CATEGORY,
            SUM(AMOUNT) AS revenue,
            COUNT(*) AS txns
        FROM {_table_fqn()}
        WHERE RUN_ID = ?
          AND {TIME_COLUMN} >= TO_TIMESTAMP_NTZ(?)
          AND {TIME_COLUMN} <= TO_TIMESTAMP_NTZ(?)
          AND {ROW_COMMIT_COLUMN} >= TO_TIMESTAMP_TZ(? || ' +0000')
          AND {ROW_COMMIT_COLUMN} <= TO_TIMESTAMP_TZ(? || ' +0000')
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    pdf = session.sql(
        query, params=[run_id, start_ntz, end_ntz, start_utc, end_utc]
    ).to_pandas()
    if pdf.empty:
        return pdf
    bucket = _find_col(pdf, "minute_bucket")
    pdf["minute_bucket"] = pd.to_datetime(pdf[bucket], utc=True, errors="coerce")
    pdf["revenue"] = _as_float(pdf[_find_col(pdf, "revenue")])
    pdf["txns"] = _as_float(pdf[_find_col(pdf, "txns")])
    pdf["category"] = pdf[_find_col(pdf, "category")]
    return pdf


def _load_resort_revenue(
    session, run_id: str, window_start: pd.Timestamp, window_end: pd.Timestamp
) -> pd.DataFrame:
    start_ntz, end_ntz = _sql_timestamp_ntz(window_start), _sql_timestamp_ntz(window_end)
    start_utc, end_utc = _sql_timestamp_tz_utc(window_start), _sql_timestamp_tz_utc(window_end)
    query = f"""
        SELECT RESORT, SUM(AMOUNT) AS revenue, COUNT(*) AS txns
        FROM {_table_fqn()}
        WHERE RUN_ID = ?
          AND {TIME_COLUMN} >= TO_TIMESTAMP_NTZ(?)
          AND {TIME_COLUMN} <= TO_TIMESTAMP_NTZ(?)
          AND {ROW_COMMIT_COLUMN} >= TO_TIMESTAMP_TZ(? || ' +0000')
          AND {ROW_COMMIT_COLUMN} <= TO_TIMESTAMP_TZ(? || ' +0000')
        GROUP BY 1
        ORDER BY revenue DESC
    """
    pdf = session.sql(
        query, params=[run_id, start_ntz, end_ntz, start_utc, end_utc]
    ).to_pandas()
    if pdf.empty:
        return pdf
    pdf["revenue"] = _as_float(pdf[_find_col(pdf, "revenue")])
    pdf["txns"] = _as_float(pdf[_find_col(pdf, "txns")])
    pdf["resort"] = pdf[_find_col(pdf, "resort")]
    return pdf


def _load_customer_payment_mix(
    session, run_id: str, window_start: pd.Timestamp, window_end: pd.Timestamp
) -> pd.DataFrame:
    start_ntz, end_ntz = _sql_timestamp_ntz(window_start), _sql_timestamp_ntz(window_end)
    start_utc, end_utc = _sql_timestamp_tz_utc(window_start), _sql_timestamp_tz_utc(window_end)
    query = f"""
        SELECT CUSTOMER_TYPE, PAYMENT_METHOD, SUM(AMOUNT) AS revenue, COUNT(*) AS txns
        FROM {_table_fqn()}
        WHERE RUN_ID = ?
          AND {TIME_COLUMN} >= TO_TIMESTAMP_NTZ(?)
          AND {TIME_COLUMN} <= TO_TIMESTAMP_NTZ(?)
          AND {ROW_COMMIT_COLUMN} >= TO_TIMESTAMP_TZ(? || ' +0000')
          AND {ROW_COMMIT_COLUMN} <= TO_TIMESTAMP_TZ(? || ' +0000')
        GROUP BY 1, 2
    """
    pdf = session.sql(
        query, params=[run_id, start_ntz, end_ntz, start_utc, end_utc]
    ).to_pandas()
    if pdf.empty:
        return pdf
    pdf["revenue"] = _as_float(pdf[_find_col(pdf, "revenue")])
    pdf["txns"] = _as_float(pdf[_find_col(pdf, "txns")])
    pdf["customer_type"] = pdf[_find_col(pdf, "customer_type")]
    pdf["payment_method"] = pdf[_find_col(pdf, "payment_method")]
    return pdf


def _load_recent_transactions(
    session, run_id: str, window_start: pd.Timestamp, limit: int
) -> pd.DataFrame:
    start_ntz = _sql_timestamp_ntz(window_start)
    query = f"""
        SELECT RESORT, CATEGORY, ITEM, CUSTOMER_TYPE, PAYMENT_METHOD, QUANTITY, AMOUNT, {TIME_COLUMN}
        FROM {_table_fqn()}
        WHERE RUN_ID = ?
          AND {TIME_COLUMN} >= TO_TIMESTAMP_NTZ(?)
        ORDER BY INGEST_TS DESC
        LIMIT {int(limit)}
    """
    pdf = session.sql(query, params=[run_id, start_ntz]).to_pandas()
    if pdf.empty:
        return pdf
    pdf["AMOUNT"] = _as_float(pdf[_find_col(pdf, "amount")])
    return pdf


def _init_widgets() -> tuple[str | None, int, int | None, int]:
    if "elastic_ramp_run_id" not in st.session_state:
        st.session_state.elastic_ramp_run_id = str(
            os.environ.get("ELASTIC_RAMP_RUN_ID", "") or ""
        )
    if "resort_sales_window" not in st.session_state:
        st.session_state.resort_sales_window = min(10, MAX_WINDOW_MINUTES)
    if "resort_sales_refresh" not in st.session_state:
        st.session_state.resort_sales_refresh = DEFAULT_REFRESH
    if "resort_sales_feed_rows" not in st.session_state:
        st.session_state.resort_sales_feed_rows = 25

    st.sidebar.header("Filters")
    run_id = st.sidebar.text_input("Run ID filter", key="elastic_ramp_run_id")
    run_id = str(run_id or "").strip() or None
    window = int(
        st.sidebar.slider(
            "Window (minutes)",
            min_value=2,
            max_value=MAX_WINDOW_MINUTES,
            key="resort_sales_window",
        )
    )
    feed_rows = int(
        st.sidebar.slider(
            "Live feed rows",
            min_value=10,
            max_value=100,
            step=5,
            key="resort_sales_feed_rows",
        )
    )
    refresh_label = st.sidebar.selectbox(
        "Auto-refresh",
        options=list(REFRESH_OPTIONS.keys()),
        key="resort_sales_refresh",
    )
    refresh_seconds = REFRESH_OPTIONS[refresh_label]
    return run_id, window, refresh_seconds, feed_rows


def _revenue_map_layer(resort_df: pd.DataFrame):
    """Sphere + graticule + bubble globe — no external tile/geometry fetch.

    Snowsight's Streamlit CSP only allow-lists Mapbox/Carto tile domains, so a
    tile basemap works too, but this stays fully self-contained: `sphere()`
    and `graticule()` are Vega-Lite data *generators* computed in the
    browser, not fetched from anywhere.
    """
    def _lat(name: str) -> float | None:
        coord = RESORT_COORDS.get(name)
        return coord[0] if coord is not None else None

    def _lon(name: str) -> float | None:
        coord = RESORT_COORDS.get(name)
        return coord[1] if coord is not None else None

    plotted = resort_df.assign(
        lat=resort_df["resort"].map(_lat), lon=resort_df["resort"].map(_lon)
    )
    plotted = plotted.dropna(subset=["lat", "lon"])
    if plotted.empty:
        return None, 0

    sphere = alt.Chart(alt.sphere()).mark_geoshape(fill="#0b1d33", stroke=None)
    graticule = alt.Chart(alt.graticule(step=[20, 20])).mark_geoshape(
        stroke="#1f3a5c", strokeWidth=0.6, fillOpacity=0
    )
    points = (
        alt.Chart(plotted)
        .mark_circle(opacity=0.85, stroke="#f8fafc", strokeWidth=0.6)
        .encode(
            longitude="lon:Q",
            latitude="lat:Q",
            size=alt.Size(
                "revenue:Q",
                title="Revenue",
                scale=alt.Scale(range=[40, 2200]),
                legend=alt.Legend(format="$,.0f"),
            ),
            color=alt.Color(
                "revenue:Q",
                title="Revenue",
                scale=alt.Scale(scheme="inferno"),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("resort:N", title="Resort"),
                alt.Tooltip("revenue:Q", title="Revenue", format="$,.2f"),
                alt.Tooltip("txns:Q", title="Transactions", format=",.0f"),
            ],
        )
    )
    chart = (
        (sphere + graticule + points)
        .project(type="naturalEarth1", rotate=[-10, 0, 0])
        .properties(width="container", height=460, background="#081525")
        .configure_view(stroke=None)
    )
    return chart, len(plotted)


def _category_area_chart(ts_df: pd.DataFrame):
    df = ts_df.copy()
    df["category_label"] = df["category"].map(lambda c: CATEGORY_META.get(c, {}).get("label", c))
    domain = [meta["label"] for meta in CATEGORY_META.values()]
    range_ = [meta["color"] for meta in CATEGORY_META.values()]
    chart = (
        alt.Chart(df)
        .mark_area(opacity=0.88, interpolate="monotone", line=True)
        .encode(
            x=alt.X("minute_bucket:T", title="Time (UTC)"),
            y=alt.Y("revenue:Q", title="Revenue / minute", stack="zero"),
            color=alt.Color(
                "category_label:N",
                title="Category",
                scale=alt.Scale(domain=domain, range=range_),
            ),
            tooltip=[
                alt.Tooltip("minute_bucket:T", title="Minute"),
                alt.Tooltip("category_label:N", title="Category"),
                alt.Tooltip("revenue:Q", title="Revenue", format="$,.2f"),
            ],
        )
        .properties(height=300)
    )
    return chart


def _category_donut(ts_df: pd.DataFrame):
    totals = ts_df.groupby("category", as_index=False)["revenue"].sum()
    totals["category_label"] = totals["category"].map(
        lambda c: CATEGORY_META.get(c, {}).get("label", c)
    )
    domain = [meta["label"] for meta in CATEGORY_META.values()]
    range_ = [meta["color"] for meta in CATEGORY_META.values()]
    chart = (
        alt.Chart(totals)
        .mark_arc(innerRadius=65, outerRadius=115, stroke="#ffffff", strokeWidth=1)
        .encode(
            theta=alt.Theta("revenue:Q", stack=True),
            color=alt.Color(
                "category_label:N",
                title="Category",
                scale=alt.Scale(domain=domain, range=range_),
            ),
            tooltip=[
                alt.Tooltip("category_label:N", title="Category"),
                alt.Tooltip("revenue:Q", title="Revenue", format="$,.2f"),
            ],
        )
        .properties(height=300)
    )
    return chart


def _leaderboard_chart(resort_df: pd.DataFrame, top_n: int = 12):
    top = resort_df.head(top_n)
    chart = (
        alt.Chart(top)
        .mark_bar(cornerRadiusEnd=4)
        .encode(
            x=alt.X("revenue:Q", title="Revenue"),
            y=alt.Y("resort:N", sort="-x", title=None),
            color=alt.Color(
                "revenue:Q", scale=alt.Scale(scheme="inferno"), legend=None
            ),
            tooltip=[
                alt.Tooltip("resort:N", title="Resort"),
                alt.Tooltip("revenue:Q", title="Revenue", format="$,.2f"),
                alt.Tooltip("txns:Q", title="Transactions", format=",.0f"),
            ],
        )
        .properties(height=26 * len(top) + 40)
    )
    return chart


def _mix_bar_chart(df: pd.DataFrame, dim_col: str, meta: dict[str, dict[str, str]]):
    grouped = df.groupby(dim_col, as_index=False)["revenue"].sum()
    grouped["label"] = grouped[dim_col].map(lambda v: meta.get(v, {}).get("label", v))
    domain = [m["label"] for m in meta.values()]
    range_ = [m["color"] for m in meta.values()]
    chart = (
        alt.Chart(grouped)
        .mark_bar(cornerRadiusEnd=4)
        .encode(
            x=alt.X("revenue:Q", title="Revenue"),
            y=alt.Y("label:N", sort="-x", title=None),
            color=alt.Color("label:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
            tooltip=[alt.Tooltip("label:N", title=""), alt.Tooltip("revenue:Q", format="$,.2f")],
        )
        .properties(height=max(120, 45 * len(grouped) + 20))
        .configure_axisY(labelLimit=160, labelFontSize=13, labelPadding=8)
        .configure_axisX(labelFontSize=11)
    )
    return chart


def _feed_dataframe(raw: pd.DataFrame) -> pd.DataFrame:
    item_col = _find_col(raw, "item")
    category_col = _find_col(raw, "category")
    resort_col = _find_col(raw, "resort")
    customer_col = _find_col(raw, "customer_type")
    payment_col = _find_col(raw, "payment_method")
    qty_col = _find_col(raw, "quantity")
    time_col = _find_col(raw, TIME_COLUMN)

    display = pd.DataFrame(
        {
            "Time (UTC)": pd.to_datetime(raw[time_col], errors="coerce"),
            "Resort": raw[resort_col],
            "Item": [
                f"{CATEGORY_META.get(cat, {}).get('emoji', '')} {item}"
                for cat, item in zip(raw[category_col], raw[item_col])
            ],
            "Customer": [
                f"{CUSTOMER_TYPE_META.get(ct, {}).get('emoji', '')} "
                f"{CUSTOMER_TYPE_META.get(ct, {}).get('label', ct)}"
                for ct in raw[customer_col]
            ],
            "Payment": [
                f"{PAYMENT_META.get(pm, {}).get('emoji', '')} "
                f"{PAYMENT_META.get(pm, {}).get('label', pm)}"
                for pm in raw[payment_col]
            ],
            "Qty": raw[qty_col],
            "Amount": raw["AMOUNT"],
        }
    )
    return display.sort_values("Time (UTC)", ascending=False)


def main() -> None:
    st.title("🏔️ Resort Sales — Real-Time")
    st.caption(
        "Season tickets, day tickets, food & drink, and merchandise sales across 42 synthetic "
        "resorts, streamed in by hundreds of elastic-channel workers and queried live from "
        "`RESORT_SALES`."
    )

    run_id, window, refresh_seconds, feed_rows = _init_widgets()

    try:
        session = get_active_session()
        _configure_session(session)
        run_id = _resolve_run_id(session, run_id)
        if not run_id:
            st.warning("No ingested rows found in RESORT_SALES yet.")
            return

        bounds = _load_run_bounds(session, run_id)
        if bounds.empty or bounds.iloc[0][_find_col(bounds, "total_txns")] in (None, 0):
            st.warning(f"No rows found for run_id={run_id}.")
            return

        first_event = _to_utc(bounds.iloc[0][_find_col(bounds, "first_event")])
        last_event = _to_utc(bounds.iloc[0][_find_col(bounds, "last_event")])
        is_live = (pd.Timestamp.now(tz="UTC") - last_event).total_seconds() <= LIVE_RUN_THRESHOLD_SECONDS
        anchor = _window_anchor(last_event)

        window_start = max(anchor - pd.Timedelta(minutes=window), first_event)
        span_start = max(anchor - pd.Timedelta(minutes=2 * window), first_event)

        ts_df = _load_category_timeseries(session, run_id, span_start, anchor)
        current_ts = ts_df[ts_df["minute_bucket"] >= window_start] if not ts_df.empty else ts_df
        previous_ts = ts_df[ts_df["minute_bucket"] < window_start] if not ts_df.empty else ts_df

        resort_df = _load_resort_revenue(session, run_id, window_start, anchor)
        mix_df = _load_customer_payment_mix(session, run_id, window_start, anchor)
        feed_start = max(anchor - pd.Timedelta(minutes=min(window, 5)), first_event)
        feed_raw = _load_recent_transactions(session, run_id, feed_start, feed_rows)
    except Exception as exc:
        st.error(f"Could not query Snowflake: {exc}")
        st.info("Ensure the Streamlit app role can SELECT from RESORT_SALES.")
        return

    status_badge = "🔴 LIVE" if is_live else "⏸️ Completed run"
    st.caption(
        f"{status_badge} · run `{run_id}` · window {_format_ts(window_start)} → "
        f"{_format_ts(anchor)} ({window} min, capped at {MAX_WINDOW_MINUTES} min)"
    )

    total_revenue = float(bounds.iloc[0][_find_col(bounds, "total_revenue")] or 0.0)
    total_txns = int(bounds.iloc[0][_find_col(bounds, "total_txns")] or 0)
    avg_ticket = float(bounds.iloc[0][_find_col(bounds, "avg_ticket")] or 0.0)

    current_revenue = float(current_ts["revenue"].sum()) if not current_ts.empty else 0.0
    previous_revenue = float(previous_ts["revenue"].sum()) if not previous_ts.empty else 0.0
    window_seconds = max(window * 60.0, 1.0)
    current_rps = current_revenue / window_seconds
    previous_rps = previous_revenue / window_seconds
    active_resorts = len(resort_df) if not resort_df.empty else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total revenue (run)", _format_usd(total_revenue))
    c1.caption(f"{total_txns:,} transactions")
    delta_rps = current_rps - previous_rps
    c2.metric(
        "Revenue / sec",
        _format_usd(current_rps),
        delta=f"{delta_rps:+,.2f}/s" if not ts_df.empty else None,
    )
    c2.caption(f"last {window} min vs prior {window} min")
    c3.metric("Avg ticket", f"${avg_ticket:,.2f}")
    c4.metric("Active resorts", f"{active_resorts} / {len(RESORT_COORDS)}")
    c4.caption(f"reporting in last {window} min")
    c5.metric(
        "Transactions / sec",
        f"{(float(current_ts['txns'].sum()) / window_seconds if not current_ts.empty else 0.0):,.1f}",
    )

    st.subheader("🗺️ Sales across the resort network")
    st.caption(
        "Bubble size and color scale with revenue in the selected window. Map projection and "
        "grid are drawn client-side (Vega-Lite `sphere`/`graticule` generators) — no external "
        "map-tile fetch, so it renders the same inside Snowsight's Streamlit sandbox."
    )
    if resort_df.empty:
        st.info("No resort revenue in the selected window yet.")
    else:
        map_chart, plotted_count = _revenue_map_layer(resort_df)
        if map_chart is None:
            st.info("No known coordinates for resorts reporting in this window.")
        else:
            st.altair_chart(map_chart, use_container_width=True)
            st.caption(f"{plotted_count} of {active_resorts} reporting resorts plotted.")

    left, right = st.columns([3, 2])
    with left:
        st.subheader("📈 Revenue trend by category")
        if current_ts.empty:
            st.info("No sales in the selected window yet.")
        else:
            st.altair_chart(_category_area_chart(current_ts), use_container_width=True)
    with right:
        st.subheader("🍩 Revenue mix")
        if current_ts.empty:
            st.info("No sales in the selected window yet.")
        else:
            st.altair_chart(_category_donut(current_ts), use_container_width=True)

    st.subheader("🏆 Top resorts by revenue")
    if resort_df.empty:
        st.info("No resort revenue in the selected window yet.")
    else:
        st.altair_chart(_leaderboard_chart(resort_df), use_container_width=True)

    st.subheader("🪪 Customer & payment mix")
    if mix_df.empty:
        st.info("No sales in the selected window yet.")
    else:
        mix_left, mix_right = st.columns(2)
        with mix_left:
            st.caption("By customer type")
            st.altair_chart(
                _mix_bar_chart(mix_df, "customer_type", CUSTOMER_TYPE_META),
                use_container_width=True,
            )
        with mix_right:
            st.caption("By payment method")
            st.altair_chart(
                _mix_bar_chart(mix_df, "payment_method", PAYMENT_META),
                use_container_width=True,
            )

    st.subheader("📜 Live transaction feed")
    st.caption(
        f"Most recent {feed_rows} rows landed (`ORDER BY INGEST_TS DESC`), scoped to the last "
        f"{min(window, 5)} minutes for a bounded scan."
    )
    if feed_raw.empty:
        st.info("No recent transactions in this window.")
    else:
        feed = _feed_dataframe(feed_raw)
        st.dataframe(
            feed,
            use_container_width=True,
            hide_index=True,
            height=min(38 * len(feed) + 40, 560),
            column_config={
                "Time (UTC)": st.column_config.DatetimeColumn(format="HH:mm:ss"),
                "Amount": st.column_config.NumberColumn(format="$%.2f"),
                "Qty": st.column_config.NumberColumn(format="%d"),
            },
        )

    updated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if refresh_seconds:
        st.caption(f"Last updated: {updated} · auto-refresh every {refresh_seconds}s")
        time.sleep(refresh_seconds)
        st.rerun()
    else:
        st.caption(f"Last updated: {updated}")


main()
