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
from typing import Callable, Dict, List, Optional

from .catalog import Catalog, Listing
from .config import CHANNEL_MARKETPLACE
from .events import (
    EVENT_SOURCE_VIRAL,
    EVENT_TYPE_ADD_TO_CART,
    EVENT_TYPE_ORDER_PLACED,
    EVENT_TYPE_PAGE_VIEW,
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


@dataclass(frozen=True)
class ViralConfig:
    listing_id: str = "LST-0017"
    page_views: int = 3000
    duration_seconds: float = 120.0
    partner_units: int = 600
    cart_rate: float = 0.3
    order_rate: float = 0.45
    seed: Optional[int] = None

    def validate(self, max_page_views: int = 20000, max_duration_seconds: int = 600) -> "ViralConfig":
        if not 1 <= self.page_views <= max_page_views:
            raise ValueError(f"page_views must be between 1 and {max_page_views}")
        if not 0 <= self.duration_seconds <= max_duration_seconds:
            raise ValueError(f"duration_seconds must be between 0 and {max_duration_seconds}")
        if self.partner_units < 1:
            raise ValueError("partner_units must be >= 1")
        return replace(self, cart_rate=min(max(self.cart_rate, 0.0), 1.0),
                       order_rate=min(max(self.order_rate, 0.0), 1.0))


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
        "referrer": rng.choice(REFERRERS),
        "postal_code": rng.choice(POSTAL_CODES),
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
        self._thread: Optional[threading.Thread] = None
        self.status = STATUS_PENDING
        self.counts: Counter = Counter()
        self.units_ordered = 0
        self.cart_units = 0
        self.published = 0
        self.failed = 0
        self.errors: List[str] = []
        self.buckets: Dict[int, Counter] = {}
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

    def join(self, timeout: Optional[float] = None) -> None:
        if self._thread:
            self._thread.join(timeout)

    @property
    def is_running(self) -> bool:
        return self.status in (STATUS_PENDING, STATUS_RUNNING)

    @property
    def seconds_since_end(self) -> float:
        return float("inf") if self._ended_at is None else self._clock() - self._ended_at

    @property
    def projected_units(self) -> int:
        return self.config.partner_units - self.units_ordered

    # ------------------------------------------------------------ generation
    def _session_events(self, ts: datetime) -> List[Dict]:
        """One shopper landing on the page: page view, maybe cart, maybe order/waitlist."""
        rng, cfg = self._rng, self.config
        session = uuid.uuid4().hex[:16]
        customer = rng.choice(self.catalog.customer_ids) if self.catalog.customer_ids else None
        common = dict(rng=rng, event_ts=ts, viral_id=self.viral_id, session_id=session, customer_id=customer,
                      listing_stock_status="IN_STOCK")
        events = [build_clickstream_event(self.catalog, self.listing, EVENT_TYPE_PAGE_VIEW, **common)]
        if rng.random() >= cfg.cart_rate:
            return events
        qty = 1 if rng.random() < 0.85 else 2
        if self.projected_units <= 0:
            events.append(build_clickstream_event(self.catalog, self.listing, EVENT_TYPE_WAITLIST_JOIN,
                                                  quantity=qty, **common))
            return events
        events.append(build_clickstream_event(self.catalog, self.listing, EVENT_TYPE_ADD_TO_CART,
                                              quantity=qty, **common))
        self.cart_units += qty
        if rng.random() < cfg.order_rate:
            events.append(build_order_event(
                self.catalog, [CartLine(self.product, qty, self.listing)], channel=CHANNEL_MARKETPLACE,
                customer_id=customer, event_source=EVENT_SOURCE_VIRAL, order_ts=ts, spike_id=self.viral_id,
            ))
            self.units_ordered += qty
        return events

    def _record(self, events: List[Dict], elapsed: float) -> None:
        bucket = self.buckets.setdefault(int(elapsed // BUCKET_SECONDS) * BUCKET_SECONDS, Counter())
        for e in events:
            self.counts[e["event_type"]] += 1
            bucket[e["event_type"]] += 1

    def _run(self) -> None:
        self.status = STATUS_RUNNING
        self._started_at = self._clock()
        try:
            i, n = 0, len(self._schedule)
            while i < n and not self._cancel.is_set():
                elapsed = self._clock() - self._started_at
                batch: List[Dict] = []
                with self._lock:
                    while i < n and self._schedule[i] <= elapsed and len(batch) < 100:
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
                    with self._lock:
                        self.units_ordered -= sum(int(li["quantity"]) for li in event["line_items"])
                if len(self.errors) < 5:
                    self.errors.append(f"{exc.__class__.__name__}: {exc}"[:300])

    # ------------------------------------------------------------ reporting
    def snapshot(self) -> Dict:
        with self._lock:
            end = self._ended_at if self._ended_at is not None else self._clock()
            elapsed = (end - self._started_at) if self._started_at is not None else 0.0
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
                "page_views": self.counts[EVENT_TYPE_PAGE_VIEW],
                "cart_units": self.cart_units,
                "add_to_cart": self.counts[EVENT_TYPE_ADD_TO_CART],
                "orders": self.counts[EVENT_TYPE_ORDER_PLACED],
                "waitlist": self.counts[EVENT_TYPE_WAITLIST_JOIN],
                "units_ordered": self.units_ordered,
                "partner_units": self.config.partner_units,
                "projected_units": self.projected_units,
                "published": self.published,
                "failed": self.failed,
                "errors": list(self.errors),
                "elapsed_seconds": round(elapsed, 1),
                "timeline": timeline,
            }
