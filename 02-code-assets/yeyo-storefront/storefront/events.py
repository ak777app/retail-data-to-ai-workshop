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

"""Order event schema shared by single orders and the spike simulator.

One JSON event per order: an order header plus a `line_items` array. Field names
match the BigQuery tables created in Module-11 so a Pub/Sub BigQuery
subscription (`--use-table-schema`) lands the event as-is.
"""

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, List, Optional, Sequence

from .catalog import DEFAULT_ONLINE_FULFILLMENT_LOCATION, Catalog, Listing, Product
from .config import CHANNEL_IN_STORE, CHANNEL_MARKETPLACE, CHANNEL_ONLINE, CHANNELS

EVENT_TYPE_ORDER_PLACED = "order_placed"
EVENT_SOURCE_STOREFRONT = "storefront"
EVENT_SOURCE_SPIKE = "spike_simulator"
SCHEMA_VERSION = "1.0"

PAYMENT_TYPES = ("CREDIT_CARD", "DEBIT_CARD", "GIFT_CARD", "DIGITAL_WALLET")
IN_STORE_PAYMENT_TYPES = PAYMENT_TYPES + ("CASH",)

FULFILLMENT_IN_STORE = "IN_STORE"
FULFILLMENT_SHIP_FROM_WAREHOUSE = "SHIP_FROM_WAREHOUSE"
FULFILLMENT_SELLER = "SELLER_FULFILLED"
FULFILLMENT_YEYO = "YEYO_FULFILLED"

_CENT = Decimal("0.01")


@dataclass(frozen=True)
class CartLine:
    product: Product
    quantity: int
    listing: Optional[Listing] = None

    @property
    def unit_price(self) -> Decimal:
        return self.listing.listing_price if self.listing else self.product.price


def format_ts(ts: datetime) -> str:
    """BigQuery-compatible UTC timestamp string."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _money(value: Decimal) -> float:
    return float(value.quantize(_CENT, rounding=ROUND_HALF_UP))


def build_order_event(
    catalog: Catalog,
    lines: Sequence[CartLine],
    channel: str,
    customer_id: Optional[str] = None,
    location_id: Optional[str] = None,
    payment_type: str = "CREDIT_CARD",
    event_source: str = EVENT_SOURCE_STOREFRONT,
    order_ts: Optional[datetime] = None,
    spike_id: Optional[str] = None,
) -> Dict:
    """Builds the order event (header + line items) for any channel."""
    if channel not in CHANNELS:
        raise ValueError(f"channel must be one of {CHANNELS}, got {channel!r}")
    if not lines:
        raise ValueError("An order needs at least one line item")

    line_items: List[Dict] = []
    sellers = set()
    total = Decimal("0")
    for n, line in enumerate(lines, start=1):
        if line.quantity < 1:
            raise ValueError(f"Quantity must be >= 1 for {line.product.omni_item_id}")
        if not line.product.sold_on(channel):
            raise ValueError(f"{line.product.omni_item_id} is not sold on channel {channel!r}")
        if channel == CHANNEL_MARKETPLACE:
            if line.listing is None:
                raise ValueError(f"Marketplace line {line.product.omni_item_id} needs a seller listing")
            if line.listing.omni_item_id != line.product.omni_item_id:
                raise ValueError("Listing does not match product omni_item_id")
        listing = line.listing if channel == CHANNEL_MARKETPLACE else None
        unit_price = listing.listing_price if listing else line.product.price
        line_total = unit_price * line.quantity
        total += line_total
        if listing:
            sellers.add(listing.seller_id)
        line_items.append({
            "line_number": n,
            "item_number": line.product.item_number,
            "omni_item_id": line.product.omni_item_id,
            "description": line.product.description,
            "category": line.product.category,
            "quantity": int(line.quantity),
            "unit_price": _money(unit_price),
            "line_item_total": _money(line_total),
            "marketplace_seller_id": listing.seller_id if listing else None,
            "marketplace_listing_id": listing.listing_id if listing else None,
        })

    if channel == CHANNEL_IN_STORE:
        stores = [s.location_id for s in catalog.stores()]
        location_id = location_id or (stores[0] if stores else None)
        fulfillment_type = FULFILLMENT_IN_STORE
    elif channel == CHANNEL_ONLINE:
        location_id = location_id or DEFAULT_ONLINE_FULFILLMENT_LOCATION
        fulfillment_type = FULFILLMENT_SHIP_FROM_WAREHOUSE
    else:
        yeyo_fulfilled = all(
            catalog.sellers[s].fulfillment_type == FULFILLMENT_YEYO for s in sellers if s in catalog.sellers
        )
        fulfillment_type = FULFILLMENT_YEYO if yeyo_fulfilled else FULFILLMENT_SELLER
        location_id = DEFAULT_ONLINE_FULFILLMENT_LOCATION if yeyo_fulfilled else None

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": EVENT_TYPE_ORDER_PLACED,
        "event_source": event_source,
        "schema_version": SCHEMA_VERSION,
        "spike_id": spike_id,
        "order_id": f"YY-{uuid.uuid4().hex[:12].upper()}",
        "order_ts": format_ts(order_ts or datetime.now(timezone.utc)),
        "channel": channel,
        "customer_id": customer_id,
        "location_id": location_id,
        "fulfillment_type": fulfillment_type,
        "marketplace_seller_id": sorted(sellers)[0] if len(sellers) == 1 else None,
        "order_status": "PLACED",
        "payment_type": payment_type,
        "currency": "USD",
        "item_count": sum(int(l.quantity) for l in lines),
        "order_total": _money(total),
        "line_items": line_items,
    }


def event_attributes(event: Dict) -> Dict[str, str]:
    """Pub/Sub message attributes; Module-11 subscriptions filter on `channel`."""
    return {
        "channel": event["channel"],
        "event_type": event["event_type"],
        "event_source": event["event_source"],
        "schema_version": event["schema_version"],
    }


def random_order_event(
    catalog: Catalog,
    rng: random.Random,
    order_ts: Optional[datetime] = None,
    marketplace_share: float = 0.4,
    max_lines: int = 3,
    spike_id: Optional[str] = None,
) -> Dict:
    """Synthetic online/marketplace order for the spike simulator."""
    channel = CHANNEL_MARKETPLACE if rng.random() < marketplace_share else CHANNEL_ONLINE
    products = catalog.products_for_channel(channel)
    if not products:
        channel = CHANNEL_ONLINE if channel == CHANNEL_MARKETPLACE else CHANNEL_MARKETPLACE
        products = catalog.products_for_channel(channel)
    if not products:
        raise ValueError("Catalog has no products sold online or on the marketplace")

    picks = rng.sample(products, k=min(len(products), rng.randint(1, max_lines)))
    lines = []
    for p in picks:
        listing = rng.choice(catalog.listings_for(p.omni_item_id)) if channel == CHANNEL_MARKETPLACE else None
        quantity = rng.choices((1, 2, 3, 4), weights=(60, 25, 10, 5))[0]
        lines.append(CartLine(product=p, quantity=quantity, listing=listing))

    return build_order_event(
        catalog,
        lines,
        channel=channel,
        customer_id=rng.choice(catalog.customer_ids) if catalog.customer_ids else None,
        payment_type=rng.choice(PAYMENT_TYPES),
        event_source=EVENT_SOURCE_SPIKE,
        order_ts=order_ts,
        spike_id=spike_id,
    )
