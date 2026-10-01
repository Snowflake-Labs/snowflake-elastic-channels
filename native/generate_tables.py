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

"""Regenerate `native/src/lib.rs` from `elastic_channels/resort_sales.py`.

The native Rust encoder doesn't independently re-implement the weighted
resort/category/item/customer-type/payment-method distribution logic —
that only needs to be correct once, in Python. Instead this script
introspects `resort_sales.py`'s own *resolved* structures (the flat,
repeat-weighted tables it already builds at import time) and emits them as
Rust literal tables, so the two encoders can never drift out of sync.

`UNIT_PRICE`/`AMOUNT` are baked in as pre-formatted decimal strings (via
Python's `json.dumps`, which matches `msgspec.json.Encoder`'s float
formatting byte-for-byte for the 2-decimal-rounded values this schema
produces) rather than left as floats for Rust to format itself — this
sidesteps any risk of Rust's float formatter or rounding disagreeing with
Python's.

This script writes `native/src/lib.rs` directly (rather than printing a
snippet to hand-copy) because the tables are large enough that manual
transcription is a real source of subtle, hard-to-spot corruption (a
mismatched array length silently changes which rows fall in each entry's
weighted bucket without breaking compilation in an obvious way).

Run this after changing any of resort_sales.py's resorts, categories,
items, prices, or weights:

    python native/generate_tables.py

Then verify parity: `PYTHONPATH=src pytest tests/unit/test_native_encoder.py`
(requires the wheel rebuilt first: `cd native && maturin develop --release`).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from elastic_channels.resort_sales import (
    _CATEGORY_ITEMS,
    _ITEM_CYCLES,
    _QUANTITY_CYCLE,
    _SALE_ENTRIES,
    CATEGORIES,
    RESORT_PROFILES,
    RESORTS,
    _build_customer_type_cycle,
)

LIB_RS = Path(__file__).resolve().parent / "src" / "lib.rs"

HEADER = '''\
//! Native NDJSON encoder for `ResortSalesGenerator` (see
//! `elastic_channels/resort_sales.py`): generate rows and write JSON bytes
//! directly in Rust, skipping per-row Python dict construction and the SDK's
//! own `msgspec.json.Encoder().encode_lines(rows)` pass entirely.
//!
//! All of the tables below (`ENTRIES`, `ITEM_CYCLE_*`, `QUANTITY_CYCLE_*`,
//! `RESORT_PRICE_TIER_ID`, `RESORT_PASS_HOLDER_THRESHOLD`, `PRICE_STRINGS_*`)
//! are *derived*, not independently re-implemented: `generate_tables.py`
//! (in this directory) generates them directly from resort_sales.py's own
//! resolved weighted structures and writes this entire file, rather than
//! them being hand-maintained, to avoid transcription errors in the large
//! generated tables. This is deliberate — the weighting logic (repeat-
//! counted flat tables, independent modular cycles for item/quantity/
//! customer-type/payment-method) only needs to be correct once, in Python;
//! Rust just replays the same resolved data.
//!
//! `UNIT_PRICE`/`AMOUNT` are pre-formatted decimal strings baked in at
//! codegen time (via Python's `json.dumps`, verified to match
//! `msgspec.json.Encoder`'s float formatting byte-for-byte) rather than
//! floats Rust formats itself — this sidesteps any risk of Rust's float
//! formatter or rounding disagreeing with Python's for the finite set of
//! (item, price_tier, quantity) combinations that actually occur.
//!
//! Row schema, resort/category/item tables, and field order must stay in
//! sync with `ResortSalesGenerator` — rerun `python native/generate_tables.py`
//! if any of `resort_sales.py`'s weights, resorts, items, or categories
//! change.

use pyo3::prelude::*;
use pyo3::types::PyBytes;

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Fixed-width, zero-padded lowercase hex — matches Python's `f"{value:0{width}x}"`
/// for any `value` that fits in `width` hex digits (true here: `value` is a u64
/// counter, so `width >= 16` never truncates).
fn write_hex_padded(buf: &mut Vec<u8>, value: u64, width: usize) {
    let start = buf.len();
    buf.resize(start + width, b'0');
    let mut v = value;
    let mut i = width;
    while v > 0 && i > 0 {
        i -= 1;
        buf[start + i] = HEX_DIGITS[(v & 0xF) as usize];
        v >>= 4;
    }
}

/// JSON-escape a string. The real vocabulary here (resort/category/item
/// names, the `SALE_TIME` timestamp) never needs escaping; only the
/// caller-supplied `run_id` could in principle contain a quote/backslash/
/// control char, so escape defensively rather than assume.
fn write_json_str(buf: &mut Vec<u8>, s: &str) {
    buf.push(b'"');
    for &b in s.as_bytes() {
        match b {
            b'"' => buf.extend_from_slice(b"\\\\\\""),
            b'\\\\' => buf.extend_from_slice(b"\\\\\\\\"),
            0x08 => buf.extend_from_slice(b"\\\\b"),
            0x0C => buf.extend_from_slice(b"\\\\f"),
            b'\\n' => buf.extend_from_slice(b"\\\\n"),
            b'\\r' => buf.extend_from_slice(b"\\\\r"),
            b'\\t' => buf.extend_from_slice(b"\\\\t"),
            0x00..=0x1F => {
                buf.extend_from_slice(format!("\\\\u{:04x}", b).as_bytes());
            }
            _ => buf.push(b),
        }
    }
    buf.push(b'"');
}

fn write_u64(buf: &mut Vec<u8>, value: u64) {
    if value == 0 {
        buf.push(b'0');
        return;
    }
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    let mut v = value;
    while v > 0 {
        i -= 1;
        tmp[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    buf.extend_from_slice(&tmp[i..]);
}

fn write_i64(buf: &mut Vec<u8>, value: i64) {
    if value < 0 {
        buf.push(b'-');
        write_u64(buf, value.unsigned_abs());
    } else {
        write_u64(buf, value as u64);
    }
}

const PAYMENT_METHODS: [&str; 4] = ["CREDIT_CARD", "PASS_CHARGE", "MOBILE_PAY", "CASH"];
const CUSTOMER_TYPE_PASS_HOLDER: &str = "PASS_HOLDER";
const CUSTOMER_TYPE_DAY_VISITOR: &str = "DAY_VISITOR";
const PASS_HOLDER_CYCLE_LEN: u64 = 20;
'''

FOOTER = '''\
/// Generate `count` rows starting after `start_counter` and encode them
/// straight to NDJSON bytes (one `{...}\\n` object per row, field order and
/// formatting matching `msgspec.json.Encoder().encode_lines()` on the
/// equivalent row dicts byte-for-byte). Returns `(ndjson_bytes, new_counter)`
/// so the caller can persist `new_counter` as its next `start_counter`.
#[pyfunction]
fn encode_batch(
    py: Python<'_>,
    start_counter: u64,
    count: u64,
    worker_id: i64,
    run_id: &str,
    sale_time: &str,
) -> PyResult<(Py<PyBytes>, u64)> {
    // ~270-320 bytes/row observed; overallocate a bit to avoid mid-batch growth.
    let mut buf: Vec<u8> = Vec::with_capacity((count as usize).saturating_mul(350));
    let mut counter = start_counter;

    for _ in 0..count {
        counter = counter.wrapping_add(1);
        let (resort_id, category_id) = ENTRIES[(counter % NUM_ENTRIES as u64) as usize];
        let (resort_id, category_id) = (resort_id as usize, category_id as usize);
        let resort = RESORT_NAMES[resort_id];
        let category = CATEGORY_NAMES[category_id];
        let tier_id = RESORT_PRICE_TIER_ID[resort_id] as usize;

        let (item_name, unit_price_str, amount_str, quantity): (&str, &str, &str, u32) =
            match category_id {
                0 => {
                    let local = ITEM_CYCLE_0[(counter % ITEM_CYCLE_0.len() as u64) as usize] as usize;
                    let qty = QUANTITY_CYCLE_0[(counter % QUANTITY_CYCLE_0.len() as u64) as usize];
                    let (u, a) = PRICE_STRINGS_0[local][tier_id][qty as usize];
                    (ITEMS_0[local], u, a, qty)
                }
                1 => {
                    let local = ITEM_CYCLE_1[(counter % ITEM_CYCLE_1.len() as u64) as usize] as usize;
                    let qty = QUANTITY_CYCLE_1[(counter % QUANTITY_CYCLE_1.len() as u64) as usize];
                    let (u, a) = PRICE_STRINGS_1[local][tier_id][qty as usize];
                    (ITEMS_1[local], u, a, qty)
                }
                2 => {
                    let local = ITEM_CYCLE_2[(counter % ITEM_CYCLE_2.len() as u64) as usize] as usize;
                    let qty = QUANTITY_CYCLE_2[(counter % QUANTITY_CYCLE_2.len() as u64) as usize];
                    let (u, a) = PRICE_STRINGS_2[local][tier_id][qty as usize];
                    (ITEMS_2[local], u, a, qty)
                }
                _ => {
                    let local = ITEM_CYCLE_3[(counter % ITEM_CYCLE_3.len() as u64) as usize] as usize;
                    let qty = QUANTITY_CYCLE_3[(counter % QUANTITY_CYCLE_3.len() as u64) as usize];
                    let (u, a) = PRICE_STRINGS_3[local][tier_id][qty as usize];
                    (ITEMS_3[local], u, a, qty)
                }
            };

        let pass_threshold = RESORT_PASS_HOLDER_THRESHOLD[resort_id] as u64;
        let customer_type = if (counter % PASS_HOLDER_CYCLE_LEN) < pass_threshold {
            CUSTOMER_TYPE_PASS_HOLDER
        } else {
            CUSTOMER_TYPE_DAY_VISITOR
        };
        let payment_method = PAYMENT_METHODS[(counter % PAYMENT_METHODS.len() as u64) as usize];

        buf.extend_from_slice(b"{\\"TXID\\":\\"");
        write_hex_padded(&mut buf, counter, 32);
        buf.extend_from_slice(b"\\",\\"RESORT\\":");
        write_json_str(&mut buf, resort);
        buf.extend_from_slice(b",\\"CATEGORY\\":");
        write_json_str(&mut buf, category);
        buf.extend_from_slice(b",\\"ITEM\\":");
        write_json_str(&mut buf, item_name);
        buf.extend_from_slice(b",\\"CUSTOMER_TYPE\\":");
        write_json_str(&mut buf, customer_type);
        buf.extend_from_slice(b",\\"PAYMENT_METHOD\\":");
        write_json_str(&mut buf, payment_method);
        buf.extend_from_slice(b",\\"QUANTITY\\":");
        write_u64(&mut buf, quantity as u64);
        buf.extend_from_slice(b",\\"UNIT_PRICE\\":");
        buf.extend_from_slice(unit_price_str.as_bytes());
        buf.extend_from_slice(b",\\"AMOUNT\\":");
        buf.extend_from_slice(amount_str.as_bytes());
        buf.extend_from_slice(b",\\"SALE_TIME\\":");
        write_json_str(&mut buf, sale_time);
        buf.extend_from_slice(b",\\"RUN_ID\\":");
        write_json_str(&mut buf, run_id);
        buf.extend_from_slice(b",\\"WORKER_ID\\":");
        write_i64(&mut buf, worker_id);
        buf.extend_from_slice(b"}\\n");
    }

    Ok((PyBytes::new(py, &buf).unbind(), counter))
}

#[pymodule]
fn elastic_channels_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_batch, m)?)?;
    Ok(())
}

// No `#[cfg(test)]` unit tests here: pyo3's `extension-module` feature omits
// libpython from the link step (Python dlopen-loads the .so instead), which
// makes `cargo test`'s standalone test binary fail to link. Correctness is
// verified from the Python side instead — see
// `tests/unit/test_native_encoder.py`, which asserts the native encoder's
// output is byte-for-byte identical to the msgspec fallback.
'''


def main() -> None:
    category_index = {c: i for i, c in enumerate(CATEGORIES)}
    resort_index = {r: i for i, r in enumerate(RESORTS)}

    distinct_tiers: list[float] = []
    for p in RESORT_PROFILES:
        if p.price_tier not in distinct_tiers:
            distinct_tiers.append(p.price_tier)
    tier_index = {t: i for i, t in enumerate(distinct_tiers)}

    resort_price_tier_id = [tier_index[p.price_tier] for p in RESORT_PROFILES]
    resort_pass_holder_threshold = [
        _build_customer_type_cycle(p.pass_holder_share).count("PASS_HOLDER") for p in RESORT_PROFILES
    ]

    item_local_index: dict[str, dict[str, int]] = {
        cat: {name: i for i, (name, _price) in enumerate(_CATEGORY_ITEMS[cat])} for cat in CATEGORIES
    }
    item_cycle_local = {
        cat: [item_local_index[cat][name] for name, _price in _ITEM_CYCLES[cat]] for cat in CATEGORIES
    }

    max_qty = max(max(_QUANTITY_CYCLE[cat]) for cat in CATEGORIES)
    entries_resolved = [(resort_index[e.resort], category_index[e.category]) for e in _SALE_ENTRIES]

    def rust_str_list(items: list[str]) -> str:
        return "[" + ", ".join(json.dumps(s) for s in items) + "]"

    out: list[str] = []
    out.append("// ===== BEGIN GENERATED (see native/generate_tables.py) =====")
    out.append(f"const NUM_CATEGORIES: usize = {len(CATEGORIES)};")
    out.append(f"const CATEGORY_NAMES: [&str; NUM_CATEGORIES] = {rust_str_list(list(CATEGORIES))};")
    out.append("")
    out.append(f"const NUM_RESORTS: usize = {len(RESORTS)};")
    out.append(f"const RESORT_NAMES: [&str; NUM_RESORTS] = {rust_str_list(list(RESORTS))};")
    out.append(
        "const RESORT_PRICE_TIER_ID: [u8; NUM_RESORTS] = "
        + "[" + ", ".join(str(x) for x in resort_price_tier_id) + "];"
    )
    out.append(
        "const RESORT_PASS_HOLDER_THRESHOLD: [u8; NUM_RESORTS] = "
        + "[" + ", ".join(str(x) for x in resort_pass_holder_threshold) + "];"
    )
    out.append("")

    for cat in CATEGORIES:
        idx = category_index[cat]
        names = [name for name, _price in _CATEGORY_ITEMS[cat]]
        out.append(f"const ITEMS_{idx}: [&str; {len(names)}] = {rust_str_list(names)};")
        cyc = item_cycle_local[cat]
        out.append(f"const ITEM_CYCLE_{idx}: [u8; {len(cyc)}] = [" + ", ".join(str(x) for x in cyc) + "];")
        qcyc = _QUANTITY_CYCLE[cat]
        out.append(f"const QUANTITY_CYCLE_{idx}: [u32; {len(qcyc)}] = [" + ", ".join(str(x) for x in qcyc) + "];")
        out.append("")

    for cat in CATEGORIES:
        idx = category_index[cat]
        items = _CATEGORY_ITEMS[cat]
        n_items = len(items)
        out.append(
            f"const PRICE_STRINGS_{idx}: [[[(&str, &str); {max_qty + 1}]; {len(distinct_tiers)}]; {n_items}] = ["
        )
        for _item_name, base_price in items:
            out.append("    [")
            for tier in distinct_tiers:
                unit_price = round(base_price * tier, 2)
                qty_entries = []
                for qty in range(max_qty + 1):
                    if qty == 0:
                        qty_entries.append('("", "")')
                        continue
                    amount = round(unit_price * qty, 2)
                    qty_entries.append(
                        f"({json.dumps(json.dumps(unit_price))}, {json.dumps(json.dumps(amount))})"
                    )
                out.append("        [" + ", ".join(qty_entries) + "],")
            out.append("    ],")
        out.append("];")
        out.append("")

    out.append(f"const NUM_ENTRIES: usize = {len(entries_resolved)};")
    out.append("const ENTRIES: [(u8, u8); NUM_ENTRIES] = [")
    line_items = [f"({r},{c})" for r, c in entries_resolved]
    for i in range(0, len(line_items), 20):
        out.append("    " + ", ".join(line_items[i:i + 20]) + ",")
    out.append("];")
    out.append("// ===== END GENERATED =====")

    assert len(line_items) == len(entries_resolved) == len(_SALE_ENTRIES), (
        len(line_items), len(entries_resolved), len(_SALE_ENTRIES)
    )

    generated_block = "\n".join(out)
    LIB_RS.write_text(HEADER + "\n" + generated_block + "\n\n" + FOOTER)
    print(f"wrote {LIB_RS}")
    print(
        f"NUM_ENTRIES={len(entries_resolved)} NUM_RESORTS={len(RESORTS)} "
        f"NUM_PRICE_TIERS={len(distinct_tiers)} MAX_QTY={max_qty}"
    )


if __name__ == "__main__":
    main()
