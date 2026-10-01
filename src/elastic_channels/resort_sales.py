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

"""Fast resort-sales record generator (RESORT_SALES schema).

Replaces the earlier lift-ride schema (`lift_ticket.py`) with point-of-sale
transactions across a synthetic 42-resort ski portfolio: season tickets, day
tickets, food & drink, and branded merchandise. Every resort gets a
deterministic sales profile (relative transaction volume, category mix,
price tier, pass-holder share) so aggregate reporting by resort is
distinctive rather than uniform-random, while row generation itself stays
allocation-light and RNG-free: a flat table of `_SaleEntry` rows is built
once at import time with each entry repeated in proportion to its target
weight, and `counter % len(table)` round-robins through it. Over a full
run the aggregate mix converges exactly to the intended weights.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

try:
    import elastic_channels_native as _native  # optional Rust extension
except ImportError:  # pragma: no cover - exercised in envs without the wheel
    _native = None

try:
    import msgspec

    _encoder = msgspec.json.Encoder()
except ImportError:  # pragma: no cover - msgspec ships with snowpipe-streaming
    msgspec = None
    _encoder = None

SEASON_TICKET = "SEASON_TICKET"
DAY_TICKET = "DAY_TICKET"
FOOD_AND_DRINK = "FOOD_AND_DRINK"
MERCHANDISE = "MERCHANDISE"
CATEGORIES: tuple[str, ...] = (SEASON_TICKET, DAY_TICKET, FOOD_AND_DRINK, MERCHANDISE)

PAYMENT_METHODS: tuple[str, ...] = ("CREDIT_CARD", "PASS_CHARGE", "MOBILE_PAY", "CASH")
CUSTOMER_TYPES: tuple[str, str] = ("PASS_HOLDER", "DAY_VISITOR")

# (item name, base price before a resort's price_tier multiplier is applied).
_CATEGORY_ITEMS: dict[str, tuple[tuple[str, float], ...]] = {
    SEASON_TICKET: (("Season Pass", 899.00),),
    DAY_TICKET: (
        ("Day Ticket - Adult", 189.00),
        ("Day Ticket - Child", 99.00),
        ("Day Ticket - Senior", 139.00),
    ),
    FOOD_AND_DRINK: (
        ("Hot Cocoa", 6.50),
        ("Coffee", 4.75),
        ("Cheeseburger", 16.00),
        ("Chili Bowl", 12.50),
        ("Pizza Slice", 8.00),
        ("Craft Beer", 11.00),
    ),
    MERCHANDISE: (
        ("Hand Warmers", 5.00),
        ("Goggles", 145.00),
        ("Gloves", 65.00),
        ("Beanie", 28.00),
        ("T-Shirt", 32.00),
        ("Sweatshirt", 58.00),
        ("Jacket", 249.00),
    ),
}

# Relative share of each item within its category, aligned to _CATEGORY_ITEMS
# order. `None` means split evenly across the category's items.
_ITEM_SHARE: dict[str, tuple[float, ...] | None] = {
    SEASON_TICKET: None,
    DAY_TICKET: (0.60, 0.25, 0.15),  # Adult, Child, Senior
    FOOD_AND_DRINK: None,
    MERCHANDISE: None,
}

# Deterministic quantity cycle per category (no single item always has qty=1
# except tickets, which are always sold one-at-a-time per transaction).
_QUANTITY_CYCLE: dict[str, tuple[int, ...]] = {
    SEASON_TICKET: (1,),
    DAY_TICKET: (1,),
    FOOD_AND_DRINK: (1, 2, 1, 3),
    MERCHANDISE: (1, 1, 2),
}


@dataclass(frozen=True)
class _ResortProfile:
    name: str
    volume_weight: float  # relative overall transaction share
    category_mix: dict[str, float]  # percentages within this resort, sum to 100
    price_tier: float  # multiplier applied to each item's base price
    pass_holder_share: float  # fraction of this resort's transactions from pass holders


# Twelve flagship properties get individually tuned profiles so their sales
# mix tells a distinct story (flagship, boutique-luxury, day-trip magnet,
# family/value, ...). Names are fictitious.
_BESPOKE_RESORTS: tuple[_ResortProfile, ...] = (
    _ResortProfile("Cedarcrest", 12.0, {SEASON_TICKET: 15, DAY_TICKET: 20, FOOD_AND_DRINK: 35, MERCHANDISE: 30}, 1.25, 0.50),
    _ResortProfile("Twinpine Alpine", 10.0, {SEASON_TICKET: 12, DAY_TICKET: 25, FOOD_AND_DRINK: 35, MERCHANDISE: 28}, 1.30, 0.40),
    _ResortProfile("Silverpine Ridge", 8.0, {SEASON_TICKET: 10, DAY_TICKET: 55, FOOD_AND_DRINK: 25, MERCHANDISE: 10}, 1.05, 0.35),
    _ResortProfile("Granite Hollow", 5.5, {SEASON_TICKET: 10, DAY_TICKET: 45, FOOD_AND_DRINK: 35, MERCHANDISE: 10}, 1.00, 0.40),
    _ResortProfile("Summit Meadows", 4.5, {SEASON_TICKET: 20, DAY_TICKET: 25, FOOD_AND_DRINK: 30, MERCHANDISE: 25}, 1.10, 0.45),
    _ResortProfile("Otter Brook", 4.0, {SEASON_TICKET: 10, DAY_TICKET: 15, FOOD_AND_DRINK: 40, MERCHANDISE: 35}, 1.20, 0.60),
    _ResortProfile("Maple Hollow", 3.3, {SEASON_TICKET: 15, DAY_TICKET: 35, FOOD_AND_DRINK: 30, MERCHANDISE: 20}, 1.15, 0.45),
    _ResortProfile("Cloudline Basin", 2.75, {SEASON_TICKET: 5, DAY_TICKET: 60, FOOD_AND_DRINK: 20, MERCHANDISE: 15}, 0.95, 0.25),
    _ResortProfile("Birch Run", 2.75, {SEASON_TICKET: 15, DAY_TICKET: 50, FOOD_AND_DRINK: 25, MERCHANDISE: 10}, 0.92, 0.35),
    _ResortProfile("Starlight Pass", 1.7, {SEASON_TICKET: 15, DAY_TICKET: 25, FOOD_AND_DRINK: 35, MERCHANDISE: 25}, 1.15, 0.50),
    _ResortProfile("Copper Finch", 0.55, {SEASON_TICKET: 25, DAY_TICKET: 40, FOOD_AND_DRINK: 25, MERCHANDISE: 10}, 0.90, 0.55),
    _ResortProfile("Ironwood Gap", 0.55, {SEASON_TICKET: 10, DAY_TICKET: 65, FOOD_AND_DRINK: 20, MERCHANDISE: 5}, 0.90, 0.20),
)

# Remaining properties, grouped into two archetypes rather than individually
# tuned (42 fictitious resorts total).
_TIER3_RESORTS: tuple[str, ...] = (
    "Frostpeak", "Blue Ledge", "Eagle Notch", "Pinecroft", "Raven Ridge",
    "Wildfern", "Glacier Bend", "Whitewater Falls", "Ember Lake", "Alpine Fork",
    "Highfield", "Seven Pines",
)
_TIER3_MIX = {SEASON_TICKET: 10.0, DAY_TICKET: 50.0, FOOD_AND_DRINK: 30.0, MERCHANDISE: 10.0}
_TIER3_PRICE_TIER = 1.00
_TIER3_PASS_HOLDER_SHARE = 0.30
_TIER3_TOTAL_VOLUME = 30.0

_TIER4_RESORTS: tuple[str, ...] = (
    "Alder Hills", "Valley Green", "Round Rock", "Mill Creek", "Cider Hollow",
    "Croft Hill", "Quiet Valley", "Shadow Glen",
    "Jackpine", "Fern Gap", "Banner Knob", "Fox Creek",
    "North Fen", "Oak Knoll", "Circle Hill", "Snowfield Creek",
    "Fox Tail", "Willowmont",
)
_TIER4_MIX = {SEASON_TICKET: 5.0, DAY_TICKET: 70.0, FOOD_AND_DRINK: 22.0, MERCHANDISE: 3.0}
_TIER4_PRICE_TIER = 0.85
_TIER4_PASS_HOLDER_SHARE = 0.15
_TIER4_TOTAL_VOLUME = 15.0


def _tier_profiles(
    names: tuple[str, ...],
    total_volume: float,
    mix: dict[str, float],
    price_tier: float,
    pass_holder_share: float,
) -> tuple[_ResortProfile, ...]:
    per_resort = total_volume / len(names)
    return tuple(
        _ResortProfile(name, per_resort, mix, price_tier, pass_holder_share)
        for name in names
    )


RESORT_PROFILES: tuple[_ResortProfile, ...] = (
    _BESPOKE_RESORTS
    + _tier_profiles(_TIER3_RESORTS, _TIER3_TOTAL_VOLUME, _TIER3_MIX, _TIER3_PRICE_TIER, _TIER3_PASS_HOLDER_SHARE)
    + _tier_profiles(_TIER4_RESORTS, _TIER4_TOTAL_VOLUME, _TIER4_MIX, _TIER4_PRICE_TIER, _TIER4_PASS_HOLDER_SHARE)
)
RESORTS: tuple[str, ...] = tuple(p.name for p in RESORT_PROFILES)


@dataclass(frozen=True)
class _SaleEntry:
    resort: str
    category: str
    price_tier: float


def _build_sale_entries(scale: float = 40.0) -> tuple[_SaleEntry, ...]:
    """Derive the flat, repeat-weighted (resort, category) lookup table.

    Every (resort, category) combination with a nonzero share appears at
    least once (so nothing is ever completely unrepresented), and otherwise
    `round(weight * scale)` times, so cycling `counter % len(table)`
    converges to the intended resort/category proportions over a full run.

    Item, customer type, and payment method are deliberately *not* folded
    into this same table: multiplying all four dimensions together produces
    thousands of combinations, most of which round down to the same
    floor-of-1 minimum and wash out the intended weighting for anything but
    the largest resorts. Instead they're picked from independent weighted
    cycles below, keyed off the same `counter` value but a different
    modulus, which keeps each dimension's resolution independent of how
    small the resort's overall volume share is.
    """
    entries: list[_SaleEntry] = []
    for profile in RESORT_PROFILES:
        for category in CATEGORIES:
            category_pct = profile.category_mix.get(category, 0.0)
            if category_pct <= 0:
                continue
            weight = profile.volume_weight * (category_pct / 100.0)
            repeat = max(1, round(weight * scale))
            entries.extend([_SaleEntry(profile.name, category, profile.price_tier)] * repeat)
    return tuple(entries)


def _build_item_cycle(category: str, scale: float = 100.0) -> tuple[tuple[str, float], ...]:
    """Weighted (item, base_price) cycle for one category, at 1/scale resolution."""
    items = _CATEGORY_ITEMS[category]
    shares = _ITEM_SHARE[category] or tuple(1.0 / len(items) for _ in items)
    cycle: list[tuple[str, float]] = []
    for (item_name, base_price), share in zip(items, shares):
        repeat = max(1, round(share * scale))
        cycle.extend([(item_name, base_price)] * repeat)
    return tuple(cycle)


def _build_customer_type_cycle(pass_holder_share: float, scale: int = 20) -> tuple[str, ...]:
    """Weighted CUSTOMER_TYPE cycle for one resort, at 1/scale resolution."""
    n_pass = round(pass_holder_share * scale)
    if 0.0 < pass_holder_share < 1.0:
        n_pass = max(1, min(scale - 1, n_pass))
    return (CUSTOMER_TYPES[0],) * n_pass + (CUSTOMER_TYPES[1],) * (scale - n_pass)


_SALE_ENTRIES: tuple[_SaleEntry, ...] = _build_sale_entries()
_SALE_ENTRY_COUNT = len(_SALE_ENTRIES)
_ITEM_CYCLES: dict[str, tuple[tuple[str, float], ...]] = {
    category: _build_item_cycle(category) for category in CATEGORIES
}
_CUSTOMER_TYPE_CYCLES: dict[str, tuple[str, ...]] = {
    profile.name: _build_customer_type_cycle(profile.pass_holder_share) for profile in RESORT_PROFILES
}


class ResortSalesGenerator:
    """Deterministic, allocation-light generator for sustained throughput tests."""

    __slots__ = (
        "_counter",
        "_entries",
        "_run_id",
        "_sale_time_sec",
        "_sale_time_str",
        "_use_native",
        "_worker_id",
    )

    def __init__(
        self,
        *,
        seed: int,
        worker_id: int = 0,
        run_id: str = "",
        use_native: bool = False,
    ) -> None:
        # seed kept for API compatibility; counter space is per-worker.
        _ = seed
        self._worker_id = worker_id
        self._run_id = run_id
        self._counter = worker_id << 32
        self._entries = _SALE_ENTRIES
        self._sale_time_sec = -1.0
        self._sale_time_str = ""
        self._use_native = use_native and _native is not None

    def _sale_time(self) -> str:
        """Reuse the same timestamp string for all rows in a ~1s window."""
        sec = time.time()
        if sec != self._sale_time_sec:
            self._sale_time_sec = sec
            self._sale_time_str = time.strftime(
                "%Y-%m-%dT%H:%M:%S", time.gmtime(sec)
            )
        return self._sale_time_str

    def next_batch(
        self,
        count: int,
        *,
        run_id: str | None = None,
        worker_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Build a batch of row dicts with minimal per-row overhead."""
        if run_id is not None:
            self._run_id = run_id
        wid = self._worker_id if worker_id is None else worker_id
        sale_time = self._sale_time()
        run_id_value = self._run_id
        entries = self._entries
        entry_count = _SALE_ENTRY_COUNT
        counter = self._counter

        batch: list[dict[str, Any]] = [None] * count  # type: ignore[list-item]
        for i in range(count):
            counter += 1
            entry = entries[counter % entry_count]
            item_cycle = _ITEM_CYCLES[entry.category]
            item_name, base_price = item_cycle[counter % len(item_cycle)]
            unit_price = round(base_price * entry.price_tier, 2)
            customer_cycle = _CUSTOMER_TYPE_CYCLES[entry.resort]
            customer_type = customer_cycle[counter % len(customer_cycle)]
            quantities = _QUANTITY_CYCLE[entry.category]
            quantity = quantities[counter % len(quantities)]
            payment_method = PAYMENT_METHODS[counter % len(PAYMENT_METHODS)]
            batch[i] = {
                "TXID": f"{counter:032x}",
                "RESORT": entry.resort,
                "CATEGORY": entry.category,
                "ITEM": item_name,
                "CUSTOMER_TYPE": customer_type,
                "PAYMENT_METHOD": payment_method,
                "QUANTITY": quantity,
                "UNIT_PRICE": unit_price,
                "AMOUNT": round(unit_price * quantity, 2),
                "SALE_TIME": sale_time,
                "RUN_ID": run_id_value,
                "WORKER_ID": wid,
            }

        self._counter = counter
        return batch

    def next_record(self) -> dict[str, Any]:
        """Single row for tests and byte estimation."""
        return self.next_batch(1)[0]

    def record_json_bytes(self, row: dict[str, Any]) -> int:
        return len(json.dumps(row, separators=(",", ":")).encode("utf-8"))

    def next_batch_ndjson(
        self,
        count: int,
        *,
        run_id: str | None = None,
        worker_id: int | None = None,
    ) -> tuple[bytes, int]:
        """Build a batch directly as NDJSON bytes (`{...}\\n` per row).

        Uses the native Rust encoder (`elastic_channels_native.encode_batch`)
        when available and enabled, generating rows and encoding NDJSON
        entirely in Rust; otherwise falls back to `next_batch()` +
        `msgspec.json.Encoder()`.
        """
        if self._use_native:
            payload, new_counter = _native.encode_batch(
                self._counter, count, worker_id or self._worker_id, self._run_id, self._sale_time()
            )
            self._counter = new_counter
            return payload, count

        batch = self.next_batch(count, run_id=run_id, worker_id=worker_id)
        if _encoder is not None:
            return _encoder.encode_lines(batch), count
        # msgspec always ships with snowpipe-streaming, but degrade gracefully
        # rather than hard-fail if it's ever missing from a minimal environment.
        lines = "".join(
            json.dumps(row, separators=(",", ":")) + "\n" for row in batch
        )
        return lines.encode("utf-8"), count


def native_encoder_available() -> bool:
    """Whether the optional `elastic_channels_native` Rust wheel is installed."""
    return _native is not None


def estimate_row_bytes(sample_count: int = 100, *, seed: int = 42) -> int:
    gen = ResortSalesGenerator(seed=seed)
    total = sum(gen.record_json_bytes(gen.next_record()) for _ in range(sample_count))
    return max(1, total // sample_count)
