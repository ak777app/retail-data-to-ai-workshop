# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The "Go viral" simulator: one marketplace SKU suddenly gets TikTok-scale traffic.

Emits page views and add-to-carts to the clickstream topic and marketplace
orders to the order topic (same schema/publisher as the storefront), on a
rising curve. Once the partner's last reported stock is used up, the listing
still says IN_STOCK, so late add-to-carts become waitlist joins.
"""

import random
import threading
from concurrent.futures import TimeoutError as FuturesTimeout
import time
import uuid
from collections import Counter
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

from .catalog import Catalog, Listing
from .config import CHANNEL_MARKETPLACE, CHANNEL_ONLINE
from .events import (
    EVENT_SOURCE_VIRAL,
    EVENT_TYPE_ADD_TO_CART,
    EVENT_TYPE_ORDER_PLACED,
    EVENT_TYPE_PAGE_VIEW,
    EVENT_TYPE_PURCHASE,
    EVENT_TYPE_WAITLIST_JOIN,
    SCHEMA_VERSION,
    CartLine,
    build_order_event,
    format_ts,
)
from .publisher import BasePublisher
from .spike import STATUS_CANCELLED, STATUS_DONE, STATUS_ERROR, STATUS_PENDING, STATUS_RUNNING

REFERRERS = ("tiktok.com", "tiktok.com", "tiktok.com", "instagram.com", "google.com", "direct")
POSTAL_CODES = ("60601", "60614", "60540", "60563", "60173", "60194", "46201", "53202")
BUCKET_SECONDS = 10
STATUS_PAUSED = "paused"


@dataclass(frozen=True)
class ViralConfig:
    listing_id: str = "LST-0017"
    page_views: int = 3000
    duration_seconds: float = 120.0
    partner_units: int = 600
    cart_rate: float = 0.3
    order_rate: float = 0.45
    seed: Optional[int] = None
    pause_at_units: Optional[int] = None
    pause_at_sell_out: bool = False
    store_stock: Tuple[Tuple[str, int], ...] = ()

    def validate(self, max_page_views: int = 20000, max_duration_seconds: int = 600) -> "ViralConfig":
        if not 1 <= self.page_views <= max_page_views:
            raise ValueError(f"page_views must be between 1 and {max_page_views}")
        if not 0 <= self.duration_seconds <= max_duration_seconds:
            raise ValueError(f"duration_seconds must be between 0 and {max_duration_seconds}")
        if self.partner_units < 1:
            raise ValueError("partner_units must be >= 1")
        stores = tuple((loc, max(0, int(units))) for loc, units in self.store_stock)
        pause = self.pause_at_units if self.pause_at_units and self.pause_at_units < self.partner_units else None
        return replace(self, cart_rate=min(max(self.cart_rate, 0.0), 1.0),
                       order_rate=min(max(self.order_rate, 0.0), 1.0), pause_at_units=pause,
                       store_stock=stores)


def viral_listing(catalog: Catalog, listing_id: str) -> Listing:
    listing = next((l for l in catalog.listings if l.listing_id == listing_id), None)
    if listing is None:
        raise ValueError(f"Unknown marketplace listing {listing_id!r}")
    return listing


def build_clickstream_event(
    catalog: Catalog,
    listing: Listing,
    event_type: str,
    rng: random.Random,
    event_ts: Optional[datetime] = None,
    viral_id: Optional[str] = None,
    session_id: Optional[str] = None,
    customer_id: Optional[str] = None,
    quantity: int = 0,
    listing_stock_status: Optional[str] = None,
    referrer: Optional[str] = None,
    postal_code: Optional[str] = None,
) -> Dict:
    product = catalog.product(listing.omni_item_id)
    seller = catalog.sellers[listing.seller_id]
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_source": EVENT_SOURCE_VIRAL,
        "schema_version": SCHEMA_VERSION,
        "event_ts": format_ts(event_ts or datetime.now(timezone.utc)),
        "viral_id": viral_id,
        "session_id": session_id or uuid.uuid4().hex[:16],
        "customer_id": customer_id,
        "channel": CHANNEL_MARKETPLACE,
        "page_type": "product_detail",
        "omni_item_id": product.omni_item_id,
        "item_number": product.item_number,
        "description": product.description,
        "marketplace_listing_id": listing.listing_id,
        "marketplace_seller_id": seller.seller_id,
        "listing_price": float(listing.listing_price),
        "listing_stock_status": listing_stock_status or listing.stock_status,
        "quantity": int(quantity),
        "referrer": referrer or rng.choice(REFERRERS),
        "postal_code": postal_code or rng.choice(POSTAL_CODES),
    }


class ViralJob:
    """Background thread that replays a rising traffic curve for one listing."""

    def __init__(
        self,
        catalog: Catalog,
        publisher: BasePublisher,
        config: ViralConfig,
        clock: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.catalog = catalog
        self.publisher = publisher
        self.config = config
        self.listing = viral_listing(catalog, config.listing_id)
        self.product = catalog.product(self.listing.omni_item_id)
        self.viral_id = f"viral-{datetime.now(timezone.utc):%Y%m%d%H%M%S}-{uuid.uuid4().hex[:6]}"
        self._clock, self._now, self._sleep = clock, now, sleep
        self._rng = random.Random(config.seed)
        self._lock = threading.Lock()
        self._cancel = threading.Event()
        self._resume = threading.Event()
        self.pause_done = False
        self.sell_out_pause_done = False
        self._paused_at: Optional[float] = None
        self._thread: Optional[threading.Thread] = None
        self.status = STATUS_PENDING
        self.counts: Counter = Counter()
        self.units_ordered = 0
        self.store_left: Dict[str, int] = dict(config.store_stock)
        self.store_units_sold = 0
        self.cart_units = 0
        self.published = 0
        self.failed = 0
        self.errors: List[str] = []
        self.buckets: Dict[int, Counter] = {}
        self.by_referrer: Dict[str, Counter] = {}
        self.by_postal: Dict[str, Counter] = {}
        self._started_at: Optional[float] = None
        self._ended_at: Optional[float] = None
        self._schedule = sorted(config.duration_seconds * (self._rng.random() ** 0.5)
                                for _ in range(config.page_views))

    # ------------------------------------------------------------ lifecycle
    def start(self) -> "ViralJob":
        self._thread = threading.Thread(target=self._run, name=f"viral-{self.viral_id}", daemon=True)
        self._thread.start()
        return self

    def cancel(self) -> None:
        self._cancel.set()

    def resume(self) -> None:
        self._resume.set()

    @property
    def is_paused(self) -> bool:
        return self.status == STATUS_PAUSED

    def join(self, timeout: Optional[float] = None) -> None:
        if self._thread:
            self._thread.join(timeout)

    @property
    def is_running(self) -> bool:
        return self.status in (STATUS_PENDING, STATUS_RUNNING, STATUS_PAUSED)

    @property
    def seconds_since_end(self) -> float:
        return float("inf") if self._ended_at is None else self._clock() - self._ended_at

    @property
    def projected_units(self) -> int:
        return self.config.partner_units - self.units_ordered

    @property
    def store_units_left(self) -> int:
        return sum(self.store_left.values())

    # ------------------------------------------------------------ generation
    def _session_events(self, ts: datetime) -> List[Dict]:
        """One shopper landing on the page: page view, maybe cart, maybe order/waitlist."""
        rng, cfg = self._rng, self.config
        session = uuid.uuid4().hex[:16]
        customer = rng.choice(self.catalog.customer_ids) if self.catalog.customer_ids else None
        common = dict(rng=rng, event_ts=ts, viral_id=self.viral_id, session_id=session, customer_id=customer,
                      listing_stock_status="IN_STOCK", referrer=rng.choice(REFERRERS),
                      postal_code=rng.choice(POSTAL_CODES))
        events = [build_clickstream_event(self.catalog, self.listing, EVENT_TYPE_PAGE_VIEW, **common)]
        if rng.random() >= cfg.cart_rate:
            return events
        qty = 1 if rng.random() < 0.85 else 2
        if self.projected_units <= 0 and self.store_units_left > 0:
            return events + self._click_and_collect_events(ts, qty, customer, common)
        if self.projected_units <= 0:
            events.append(build_clickstream_event(self.catalog, self.listing, EVENT_TYPE_WAITLIST_JOIN,
                                                  quantity=qty, **common))
            return events
        qty = min(qty, self.projected_units)
        events.append(build_clickstream_event(self.catalog, self.listing, EVENT_TYPE_ADD_TO_CART,
                                              quantity=qty, **common))
        self.cart_units += qty
        if rng.random() < cfg.order_rate:
            events.append(build_order_event(
                self.catalog, [CartLine(self.product, qty, self.listing)], channel=CHANNEL_MARKETPLACE,
                customer_id=customer, event_source=EVENT_SOURCE_VIRAL, order_ts=ts, spike_id=self.viral_id,
            ))
            events.append(build_clickstream_event(self.catalog, self.listing, EVENT_TYPE_PURCHASE,
                                                  quantity=qty, **common))
            self.units_ordered += qty
        return events

    def _click_and_collect_events(self, ts: datetime, qty: int, customer: Optional[str], common: Dict) -> List[Dict]:
        """Partner sold out: ads send the shopper to collect from a YeYo store that still has stock."""
        rng = self._rng
        events = [build_clickstream_event(self.catalog, self.listing, EVENT_TYPE_ADD_TO_CART, quantity=qty, **common)]
        self.cart_units += qty
        if rng.random() >= self.config.order_rate:
            return events
        store = rng.choice([loc for loc, left in self.store_left.items() if left > 0])
        qty = min(qty, self.store_left[store])
        events.append(build_order_event(
            self.catalog, [CartLine(self.product, qty)], channel=CHANNEL_ONLINE, customer_id=customer,
            location_id=store, event_source=EVENT_SOURCE_VIRAL, order_ts=ts, spike_id=self.viral_id,
            click_and_collect=True,
        ))
        events.append(build_clickstream_event(self.catalog, self.listing, EVENT_TYPE_PURCHASE, quantity=qty, **common))
        self.store_left[store] -= qty
        self.store_units_sold += qty
        return events

    def _record(self, events: List[Dict], elapsed: float) -> None:
        bucket = self.buckets.setdefault(int(elapsed // BUCKET_SECONDS) * BUCKET_SECONDS, Counter())
        for e in events:
            self.counts[e["event_type"]] += 1
            bucket[e["event_type"]] += 1
            if "referrer" in e:
                for dim, key in ((self.by_referrer, e["referrer"]), (self.by_postal, e["postal_code"])):
                    c = dim.setdefault(key, Counter())
                    c[e["event_type"]] += 1
                    c[f"{e['event_type']}_units"] += e["quantity"]

    def _should_pause_at_units(self) -> bool:
        limit = self.config.pause_at_units
        return limit is not None and not self.pause_done and self.projected_units <= limit

    def _should_pause_at_sell_out(self) -> bool:
        """Partner sold out, ads now send shoppers to store click & collect: hold before the first store order."""
        return (self.config.pause_at_sell_out and not self.sell_out_pause_done
                and self.projected_units <= 0 and self.store_units_left > 0)

    def _should_pause(self) -> bool:
        return self._should_pause_at_units() or self._should_pause_at_sell_out()

    def _pause(self) -> None:
        """Hold the spike (presenter talks over the dashboards) until resume() or cancel()."""
        with self._lock:
            if self._should_pause_at_sell_out():
                self.sell_out_pause_done = True
            if self._should_pause_at_units():
                self.pause_done = True
            self._resume.clear()
            self._paused_at = self._clock()
            self.status = STATUS_PAUSED
        while not self._resume.wait(0.25) and not self._cancel.is_set():
            pass
        with self._lock:
            self._started_at += self._clock() - self._paused_at
            self._paused_at = None
            self.status = STATUS_RUNNING

    def _run(self) -> None:
        self.status = STATUS_RUNNING
        self._started_at = self._clock()
        try:
            i, n = 0, len(self._schedule)
            while i < n and not self._cancel.is_set():
                if self._should_pause():
                    self._pause()
                    continue
                elapsed = self._clock() - self._started_at
                batch: List[Dict] = []
                with self._lock:
                    while (i < n and self._schedule[i] <= elapsed and len(batch) < 100
                           and not self._should_pause()):
                        events = self._session_events(self._now())
                        self._record(events, elapsed)
                        batch.extend(events)
                        i += 1
                if batch:
                    self._publish(batch)
                else:
                    self._sleep(min(0.25, max(0.0, self._schedule[i] - elapsed)))
            self.publisher.flush()
            self.status = STATUS_CANCELLED if self._cancel.is_set() else STATUS_DONE
        except Exception as exc:  # pragma: no cover - defensive
            self.errors.append(f"{exc.__class__.__name__}: {exc}")
            self.status = STATUS_ERROR
        finally:
            self._ended_at = self._clock()

    def _publish(self, batch: List[Dict]) -> None:
        for event, future in zip(batch, self.publisher.publish_many(batch)):
            try:
                future.result(timeout=60)
                self.published += 1
            except Exception as exc:
                self.failed += 1
                if event["event_type"] == EVENT_TYPE_ORDER_PLACED and not isinstance(exc, FuturesTimeout):
                    units = sum(int(li["quantity"]) for li in event["line_items"])
                    with self._lock:
                        if event["channel"] == CHANNEL_ONLINE and event.get("location_id") in self.store_left:
                            self.store_left[event["location_id"]] += units
                            self.store_units_sold -= units
                        else:
                            self.units_ordered -= units
                if len(self.errors) < 5:
                    self.errors.append(f"{exc.__class__.__name__}: {exc}"[:300])

    # ------------------------------------------------------------ reporting
    def snapshot(self) -> Dict:
        with self._lock:
            end = next((t for t in (self._ended_at, self._paused_at) if t is not None), None) or self._clock()
            elapsed = (end - self._started_at) if self._started_at is not None else 0.0
            live = sum(c[EVENT_TYPE_PAGE_VIEW] for s, c in self.buckets.items() if s + BUCKET_SECONDS > elapsed - 60)
            timeline = [
                {"second": s, "page_views": c[EVENT_TYPE_PAGE_VIEW], "add_to_cart": c[EVENT_TYPE_ADD_TO_CART],
                 "orders": c[EVENT_TYPE_ORDER_PLACED], "waitlist": c[EVENT_TYPE_WAITLIST_JOIN]}
                for s, c in sorted(self.buckets.items())
            ]
            return {
                "viral_id": self.viral_id,
                "status": self.status,
                "listing_id": self.listing.listing_id,
                "omni_item_id": self.product.omni_item_id,
                "description": self.product.description,
                "seller_name": self.catalog.sellers[self.listing.seller_id].seller_name,
                "paused": self.status == STATUS_PAUSED,
                "pause_done": self.pause_done,
                "sell_out_pause_done": self.sell_out_pause_done,
                "live_shoppers": live,
                "page_views": self.counts[EVENT_TYPE_PAGE_VIEW],
                "cart_units": self.cart_units,
                "add_to_cart": self.counts[EVENT_TYPE_ADD_TO_CART],
                "orders": self.counts[EVENT_TYPE_ORDER_PLACED],
                "waitlist": self.counts[EVENT_TYPE_WAITLIST_JOIN],
                "purchases": self.counts[EVENT_TYPE_PURCHASE],
                "by_referrer": {k: dict(c) for k, c in self.by_referrer.items()},
                "by_postal": {k: dict(c) for k, c in self.by_postal.items()},
                "units_ordered": self.units_ordered,
                "partner_units": self.config.partner_units,
                "projected_units": self.projected_units,
                "store_units_total": sum(units for _, units in self.config.store_stock),
                "store_units_left": self.store_units_left,
                "store_left": dict(self.store_left),
                "store_units_sold": self.store_units_sold,
                "units_sold_total": self.units_ordered + self.store_units_sold,
                "published": self.published,
                "failed": self.failed,
                "errors": list(self.errors),
                "elapsed_seconds": round(elapsed, 1),
                "timeline": timeline,
            }
