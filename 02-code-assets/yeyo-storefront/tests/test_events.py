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

import random

import pytest

from storefront.catalog import product_from_row, synthesize_listings
from storefront.events import CartLine, build_order_event, event_attributes, random_order_event


def test_sample_catalog_is_pharmacy_and_lifestyle(catalog):
    assert {"pharmacy", "lifestyle"} <= set(catalog.categories())
    assert all(p.is_active for p in catalog.products)
    assert catalog.products_for_channel("marketplace")
    assert all(catalog.listings_for(p.omni_item_id) for p in catalog.products_for_channel("marketplace"))
    # pharmacy-counter item is in-store only; inactive seller listings are hidden
    assert "PHA-1006-OM" not in {p.omni_item_id for p in catalog.products_for_channel("online")}
    assert all(l.seller_id != "MKT-S006" for l in catalog.listings_for("PHA-1001-OM"))


def test_product_from_original_product_master_columns():
    p = product_from_row({"item_number": "X1", "appliance_type": "refrigerator", "omni_item_id": "X1",
                          "description": "Fridge", "price": "10", "is_active": "Y"})
    assert p.category == "refrigerator" and p.channels == ("in_store", "online", "marketplace")


def test_synthesized_listings_cover_marketplace_products(catalog):
    listings = synthesize_listings(catalog.products, catalog.sellers)
    assert {l.omni_item_id for l in listings} == {p.omni_item_id for p in catalog.products if p.sold_on("marketplace")}


def test_online_order_event(catalog):
    p = catalog.product("PHA-1001-OM")
    ev = build_order_event(catalog, [CartLine(p, 2)], channel="online", customer_id="c1")
    assert ev["channel"] == "online" and ev["location_id"] == "WHE-IL-WH"
    assert ev["order_total"] == pytest.approx(17.98) and ev["item_count"] == 2
    line = ev["line_items"][0]
    assert line["omni_item_id"] == "PHA-1001-OM" and line["marketplace_seller_id"] is None
    assert event_attributes(ev)["channel"] == "online"
    assert ev["order_ts"].endswith("Z")


def test_marketplace_order_uses_listing_price_and_seller(catalog):
    p = catalog.product("PHA-1012-OM")
    listing = next(l for l in catalog.listings_for(p.omni_item_id) if l.seller_id == "MKT-S002")
    ev = build_order_event(catalog, [CartLine(p, 1, listing)], channel="marketplace")
    line = ev["line_items"][0]
    assert line["marketplace_seller_id"] == "MKT-S002" and line["marketplace_listing_id"] == listing.listing_id
    assert line["unit_price"] == float(listing.listing_price)
    assert ev["fulfillment_type"] == "SELLER_FULFILLED" and ev["location_id"] is None
    assert ev["marketplace_seller_id"] == "MKT-S002"


def test_marketplace_order_requires_listing(catalog):
    with pytest.raises(ValueError):
        build_order_event(catalog, [CartLine(catalog.product("PHA-1001-OM"), 1)], channel="marketplace")


def test_channel_restrictions(catalog):
    with pytest.raises(ValueError):
        build_order_event(catalog, [CartLine(catalog.product("PHA-1006-OM"), 1)], channel="online")
    ev = build_order_event(catalog, [CartLine(catalog.product("PHA-1006-OM"), 1)], channel="in_store",
                           location_id="NAP-IL-ST")
    assert ev["location_id"] == "NAP-IL-ST" and ev["fulfillment_type"] == "IN_STORE"


def test_random_orders_are_online_or_marketplace(catalog):
    rng = random.Random(7)
    events = [random_order_event(catalog, rng, marketplace_share=0.5) for _ in range(200)]
    assert {e["channel"] for e in events} == {"online", "marketplace"}
    for e in events:
        assert e["event_source"] == "spike_simulator"
        for line in e["line_items"]:
            assert (line["marketplace_seller_id"] is not None) == (e["channel"] == "marketplace")


def test_mixed_fulfillment_marketplace_order_keeps_line_fulfillment(catalog):
    picks = {}
    for p in catalog.products_for_channel("marketplace"):
        for listing in catalog.listings_for(p.omni_item_id):
            picks.setdefault(catalog.sellers[listing.seller_id].fulfillment_type, CartLine(p, 1, listing))
    ev = build_order_event(catalog, [picks["YEYO_FULFILLED"], picks["SELLER_FULFILLED"]], channel="marketplace")
    assert [li["fulfillment_type"] for li in ev["line_items"]] == ["YEYO_FULFILLED", "SELLER_FULFILLED"]
    assert ev["fulfillment_type"] == "SELLER_FULFILLED" and ev["location_id"] == "WHE-IL-WH"


def test_click_and_collect_order(catalog):
    from storefront.events import CartLine, build_order_event
    p = catalog.product("BEA-3001-OM")
    ev = build_order_event(catalog, [CartLine(p, 1)], channel="online", location_id="CHI-IL-ST",
                           click_and_collect=True)
    assert ev["fulfillment_type"] == "CLICK_AND_COLLECT" and ev["location_id"] == "CHI-IL-ST"
    assert ev["line_items"][0]["fulfillment_type"] == "CLICK_AND_COLLECT"
    with pytest.raises(ValueError):
        build_order_event(catalog, [CartLine(p, 1)], channel="online", click_and_collect=True)
