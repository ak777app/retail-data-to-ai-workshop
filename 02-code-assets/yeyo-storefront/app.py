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

"""YeYo mock pharmacy & lifestyle storefront (Streamlit).

Run:  streamlit run app.py --server.port 8080
"""

import json
from datetime import datetime, timezone

import streamlit as st

from storefront.catalog import Catalog, load_catalog
from storefront.config import CHANNEL_IN_STORE, CHANNEL_MARKETPLACE, CHANNEL_ONLINE, Config, load_config
from storefront.events import IN_STORE_PAYMENT_TYPES, PAYMENT_TYPES, CartLine, build_order_event
from storefront.marketing import (
    PHASE_BASELINE,
    by_postcode_sql,
    by_referrer_sql,
    campaign_bids,
    simulator_rows,
    story_phase,
    totals,
)
from storefront.publisher import AutoPublisher, BasePublisher, create_publisher
from storefront.ops import (
    demo_links,
    group_links,
    latest_clickstream_sql,
    oversell_sql,
    per_minute_sql,
    record_alert_sql,
    reset_store_stock,
    risk_level,
    run_query,
    store_stock_sql,
)
from storefront.ops import (TERMINAL_BATCH_STATES, AwsRestock, PartnerListing, aws_restock_enabled, batch_state,
                            submit_restock)
from storefront import agent as data_agent
from storefront.viral import ViralConfig, ViralJob, viral_listing

CHANNEL_LABELS = {
    CHANNEL_IN_STORE: "In-store",
    CHANNEL_ONLINE: "Online",
    CHANNEL_MARKETPLACE: "Marketplace",
}

st.set_page_config(page_title="YeYo Storefront", layout="wide")


@st.cache_resource
def get_config() -> Config:
    return load_config()


@st.cache_resource
def get_catalog(_config: Config) -> Catalog:
    return load_catalog(_config)


@st.cache_resource
def get_publisher(_config: Config) -> BasePublisher:
    return create_publisher(_config)


config = get_config()
catalog = get_catalog(config)
publisher = get_publisher(config)

ss = st.session_state
ss.setdefault("cart", [])
ss.setdefault("orders", [])
ss.setdefault("viral_job", None)
ss.setdefault("restock", None)
ss.setdefault("ops_bq", {})
ss.setdefault("mkt_bq", {})
ss.setdefault("hero_sold_out", None)
ss.setdefault("open_dialog", None)
ss.setdefault("channel", CHANNEL_ONLINE)


def _money(v) -> str:
    return f"${float(v):,.2f}"


def _clear_cart() -> None:
    ss.cart = []


def _add_to_cart(omni_item_id: str) -> None:
    qty = int(ss.get(f"qty_{omni_item_id}", 1))
    listing_id = ss.get(f"seller_{omni_item_id}") if ss.channel == CHANNEL_MARKETPLACE else None
    for line in ss.cart:
        if line["omni_item_id"] == omni_item_id and line["listing_id"] == listing_id:
            line["quantity"] += qty
            return
    ss.cart.append({"omni_item_id": omni_item_id, "quantity": qty, "listing_id": listing_id})


def _remove_line(index: int) -> None:
    ss.cart.pop(index)


def _cart_lines():
    listings = {l.listing_id: l for l in catalog.listings}
    return [
        CartLine(catalog.product(l["omni_item_id"]), l["quantity"], listings.get(l["listing_id"]))
        for l in ss.cart
    ]


def _seller_label(listing_id: str) -> str:
    listing = next(l for l in catalog.listings if l.listing_id == listing_id)
    seller = catalog.sellers[listing.seller_id]
    return f"{seller.seller_name} - {_money(listing.listing_price)} ({seller.rating:.1f}/5)"


def _place_order() -> None:
    lines = _cart_lines()
    if ss.channel != CHANNEL_IN_STORE and any(l.product.omni_item_id == ss.hero_sold_out for l in lines):
        ss.last_order = {"error": "Sold out online and on the Marketplace: remove it from the cart and use "
                                  "Click & collect."}
        return
    try:
        event = build_order_event(
            catalog,
            lines,
            channel=ss.channel,
            customer_id=ss.get("customer_id"),
            location_id=ss.get("store_id") if ss.channel == CHANNEL_IN_STORE else None,
            payment_type=ss.get("payment_type", "CREDIT_CARD"),
        )
        message_id = publisher.publish(event).result(timeout=30)
    except Exception as exc:
        ss.last_order = {"error": f"{exc.__class__.__name__}: {exc}"}
        return
    ss.last_order = {"event": event, "message_id": message_id}
    ss.orders = ([event] + ss.orders)[:50]
    ss.cart = []


def _store_name(location_id: str) -> str:
    return next((s.location_name for s in catalog.locations if s.location_id == location_id), location_id)


def _stores_with_stock() -> list:
    """YeYo stores holding the sold-out SKU: the viral run's store stock, else BigQuery, else all stores."""
    job = ss.get("viral_job")
    if job is not None and job.config.store_stock:
        return [(loc, left) for loc, left in job.snapshot()["store_left"].items() if left > 0]
    rows = [r for r in (ss.ops_bq.get("stores") or []) if r["location_id"].endswith("-ST") and r["quantity_on_hand"] > 0]
    return [(r["location_id"], int(r["quantity_on_hand"])) for r in rows] or [(s.location_id, None)
                                                                                for s in catalog.stores()]


def _click_and_collect(omni_item_id: str) -> None:
    try:
        event = build_order_event(
            catalog, [CartLine(catalog.product(omni_item_id), int(ss.get(f"qty_{omni_item_id}", 1)))],
            channel=CHANNEL_ONLINE, customer_id=ss.get("customer_id"),
            location_id=ss.get(f"collect_{omni_item_id}"), payment_type=ss.get("payment_type", "CREDIT_CARD"),
            click_and_collect=True)
        message_id = publisher.publish(event).result(timeout=30)
    except Exception as exc:
        ss.last_order = {"error": f"{exc.__class__.__name__}: {exc}"}
        return
    ss.last_order = {"event": event, "message_id": message_id}
    ss.orders = ([event] + ss.orders)[:50]


# ---------------------------------------------------------------- sidebar
with st.sidebar:
    st.markdown("## YeYo")
    st.caption("Pharmacy & lifestyle - mock storefront")
    st.radio(
        "Channel",
        options=list(CHANNEL_LABELS),
        format_func=CHANNEL_LABELS.get,
        key="channel",
        on_change=_clear_cart,
        horizontal=True,
    )
    customers = catalog.customer_ids or [None]
    st.selectbox("Shopper (customer_id)", customers, key="customer_id")
    if ss.channel == CHANNEL_IN_STORE:
        stores = catalog.stores()
        st.selectbox("Store", [s.location_id for s in stores], key="store_id",
                     format_func=lambda i: next(s.location_name for s in stores if s.location_id == i))
    st.selectbox("Payment", IN_STORE_PAYMENT_TYPES if ss.channel == CHANNEL_IN_STORE else PAYMENT_TYPES,
                 key="payment_type")

    st.divider()
    st.markdown("**Streaming target**")
    st.caption(f"Project: `{config.project_id or '(not set)'}` - Location: `{config.location}`")
    st.caption(f"Publisher: {publisher.describe()}")
    if isinstance(publisher, AutoPublisher) and publisher.using_fallback:
        st.warning("Pub/Sub unavailable - writing directly to BigQuery.")
    st.caption(f"Catalog: {catalog.source} ({len(catalog.products)} products, "
               f"{len([s for s in catalog.sellers.values() if s.is_active])} sellers)")
    for w in catalog.warnings:
        st.caption(f":orange[{w}]")

st.title("YeYo")
st.caption("Everyday pharmacy, wellness and lifestyle - in-store, online and on the YeYo Marketplace.")

OPS_TAB = "Ops: viral moment"
shop_tab, ops_tab, marketing_tab, agent_tab = st.tabs(["Shop", OPS_TAB, "Marketing: viral moment",
                                                       "Ask the data agent"], key="nav",
                                           on_change="rerun")
# Coming back to the Ops tab while the spike is paused resumes it (no button needed on stage).
_nav, _prev_nav = ss.get("nav"), ss.get("prev_nav")
ss.prev_nav = _nav
if ss.viral_job is not None and ss.viral_job.is_paused and _nav == OPS_TAB and _prev_nav not in (None, OPS_TAB):
    ss.viral_job.resume()

# ---------------------------------------------------------------- shop
with shop_tab:
    grid_col, cart_col = st.columns([3, 1], gap="large")

    with grid_col:
        f1, f2 = st.columns([1, 2])
        category = f1.selectbox("Category", ["All"] + catalog.categories())
        search = f2.text_input("Search", placeholder="e.g. allergy, vitamin, yoga").strip().lower()
        products = [
            p for p in catalog.products_for_channel(ss.channel)
            if (category == "All" or p.category == category)
            and (not search or search in f"{p.description} {p.brand} {p.sub_category}".lower())
        ]
        if not products:
            st.info(f"No products available on the {CHANNEL_LABELS[ss.channel]} channel for this filter.")
        cols = st.columns(3)
        for i, p in enumerate(products):
            with cols[i % 3].container(border=True):
                st.markdown(f"**{p.description}**")
                st.caption(f"{p.brand} - {p.category.replace('_', ' ')} / {p.sub_category.replace('_', ' ')}")
                st.caption(f"`{p.omni_item_id}`")
                if ss.hero_sold_out == p.omni_item_id and ss.channel != CHANNEL_IN_STORE:
                    st.markdown(f"### {_money(p.price)}")
                    st.warning("Sold out online and on the Marketplace. Click & collect from a YeYo store only.")
                    stock = dict(_stores_with_stock())
                    if not stock:
                        st.error("Sold out in every YeYo store too.")
                        continue
                    st.selectbox("Collect at", list(stock), key=f"collect_{p.omni_item_id}",
                                 format_func=lambda i: _store_name(i) + (f" ({stock[i]} in stock)" if stock[i] else ""))
                    q, b = st.columns([1, 1])
                    q.number_input("Qty", min_value=1, max_value=20, value=1, key=f"qty_{p.omni_item_id}",
                                   label_visibility="collapsed")
                    b.button("Click & collect", key=f"collect_btn_{p.omni_item_id}", type="primary",
                             on_click=_click_and_collect, args=(p.omni_item_id,), width="stretch")
                    continue
                if ss.channel == CHANNEL_MARKETPLACE:
                    listing_ids = [l.listing_id for l in catalog.listings_for(p.omni_item_id)]
                    st.selectbox("Seller", listing_ids, key=f"seller_{p.omni_item_id}",
                                 format_func=_seller_label)
                else:
                    st.markdown(f"### {_money(p.price)}")
                q, b = st.columns([1, 1])
                q.number_input("Qty", min_value=1, max_value=20, value=1, key=f"qty_{p.omni_item_id}",
                               label_visibility="collapsed")
                b.button("Add to cart", key=f"add_{p.omni_item_id}", on_click=_add_to_cart,
                         args=(p.omni_item_id,), width="stretch")

    with cart_col:
        st.subheader(f"Cart - {CHANNEL_LABELS[ss.channel]}")
        lines = _cart_lines()
        if not lines:
            st.caption("Your cart is empty.")
        total = 0.0
        for idx, line in enumerate(lines):
            subtotal = float(line.unit_price) * line.quantity
            total += subtotal
            with st.container(border=True):
                st.markdown(f"**{line.product.description}**")
                seller = f" - sold by {catalog.sellers[line.listing.seller_id].seller_name}" if line.listing else ""
                st.caption(f"{line.quantity} x {_money(line.unit_price)} = {_money(subtotal)}{seller}")
                st.button("Remove", key=f"rm_{idx}", on_click=_remove_line, args=(idx,))
        st.markdown(f"**Total: {_money(total)}**")
        st.button("Place order", type="primary", disabled=not lines, on_click=_place_order,
                  width="stretch")

        last = ss.get("last_order")
        if last and "error" in last:
            st.error(f"Order not published: {last['error']}")
        elif last:
            ev = last["event"]
            st.success(f"Order {ev['order_id']} published ({CHANNEL_LABELS[ev['channel']]}, "
                       f"{_money(ev['order_total'])}). Message ID: {last['message_id']}")
            with st.expander("Order event JSON"):
                st.code(json.dumps(ev, indent=2), language="json")

# ---------------------------------------------------------------- ops
def _substitute_product(omni_item_id: str):
    hero = catalog.product(omni_item_id)
    same_type = [p for p in catalog.products if p.omni_item_id != omni_item_id
                 and p.category == hero.category and p.is_active]
    own_brand = [p for p in same_type if "YEYO" in (p.brand or "").upper()]
    return (own_brand or same_type or [None])[0]


def _ops_bigquery(job: ViralJob) -> None:
    """Refresh the BigQuery-backed panels (oversell view, per-minute chart, store stock)."""
    if config.publish_mode == "local":
        return
    cache = ss.ops_bq
    now = datetime.now(timezone.utc)
    if cache.get("at") and (now - cache["at"]).total_seconds() < 10:
        return
    cache["at"] = now
    params = {"listing_id": job.listing.listing_id, "omni_item_id": job.product.omni_item_id}
    for key, sql in (("risk", oversell_sql(config)), ("minutes", per_minute_sql(config)),
                     ("stores", store_stock_sql(config)), ("latest", latest_clickstream_sql(config))):
        try:
            cache[key] = run_query(config, sql, params)
            cache.pop(f"{key}_error", None)
        except Exception as exc:
            cache[f"{key}_error"] = f"{exc.__class__.__name__}: {str(exc)[:200]}"
    risk = (cache.get("risk") or [{}])[0].get("risk_level")
    if risk and risk != "OK" and cache.get("alerted") != (job.viral_id, risk):
        try:
            run_query(config, record_alert_sql(config), {"listing_id": job.listing.listing_id,
                                                         "viral_id": job.viral_id})
            cache["alerted"] = (job.viral_id, risk)
        except Exception as exc:
            cache["alert_error"] = f"{exc.__class__.__name__}: {str(exc)[:200]}"


def _clear_run() -> None:
    ss.viral_job = None
    ss.ops_bq = {}
    ss.mkt_bq = {}
    ss.hero_sold_out = None
    ss.open_dialog = None


def _bids_table(phase: str, omni_item_id: str) -> None:
    hero = catalog.product(omni_item_id)
    others, seen = [], {hero.category}
    for p in sorted(catalog.products, key=lambda p: p.omni_item_id):
        if p.is_active and p.category not in seen and len(others) < 3:
            seen.add(p.category)
            others.append((p.description, p.sub_category.replace("_", " ")))
    sub = _substitute_product(omni_item_id)
    rows = campaign_bids(phase, hero.description, others, sub.description if sub else None,
                         [_store_name(s) for s, _ in _stores_with_stock()])
    st.markdown("**Marketing bids right now** (simulated: Google Ads is not connected)")
    st.dataframe(rows, hide_index=True, width="stretch")


def _phase(snap: dict) -> str:
    return story_phase(True, snap["pause_done"], snap["projected_units"],
                       snap["store_units_left"] if snap["store_units_total"] else None)


def _finish_card(snap: dict) -> None:
    partner, stores = snap["units_ordered"], snap["store_units_sold"]
    total = partner + stores
    with st.container(border=True):
        st.markdown(f"**Units sold: {total:,}** = {partner:,} on the Marketplace + **{stores:,} click & collect** "
                    f"from London stores ({snap['store_units_left']:,} store units left)")
        if partner:
            st.markdown(f"Without the lakehouse: the same spike sells {partner:,} units, then empty shelves online "
                        f"while the stores sit on stock. With it: **{total / partner:.1f}× the units**.")


def _aws_restock(units: int) -> AwsRestock:
    seller_id = hero_listing.seller_id
    listings = [PartnerListing(x.listing_id, x.seller_id, x.omni_item_id, catalog.product(x.omni_item_id).item_number)
                for x in catalog.listings if x.seller_id == seller_id]
    stock = {x.listing_id: units if x.listing_id == hero_listing.listing_id else 1500 for x in listings}
    return AwsRestock(config, listings, stock).start()


def _reset_demo(units: int, store_units: int) -> None:
    """Clear the viral run, restock the London stores and the partner (Spark Iceberg snapshot, or S3 via Omni)."""
    _clear_run()
    if config.publish_mode != "local" and hero_listing is not None:
        hero_product = catalog.product(hero_listing.omni_item_id)
        try:
            reset_store_stock(config, hero_product.item_number, hero_product.omni_item_id,
                              [s.location_id for s in catalog.stores()], store_units)
        except Exception as exc:
            st.warning(f"Store stock not reset in BigQuery: {exc.__class__.__name__}: {str(exc)[:200]}")
    if config.publish_mode != "local" and hero_listing is not None and aws_restock_enabled(config):
        ss.restock = {"job": _aws_restock(units), "state": "PENDING", "units": units}
        return
    if config.publish_mode == "local" or not config.lakehouse_bucket:
        ss.restock = {"state": "SKIPPED", "units": units}
        return
    try:
        ss.restock = {"name": submit_restock(config, units), "state": "PENDING", "units": units}
    except Exception as exc:
        ss.restock = {"state": "FAILED", "units": units, "error": f"{exc.__class__.__name__}: {str(exc)[:200]}"}


with ops_tab:
    try:
        hero_listing = viral_listing(catalog, config.viral_listing_id)
    except ValueError as exc:
        st.error(str(exc))
        hero_listing = None
    if hero_listing is not None:
        hero = catalog.product(hero_listing.omni_item_id)
        seller = catalog.sellers[hero_listing.seller_id]
        st.markdown(f"### {hero.description}")
        st.caption(f"`{hero.omni_item_id}` on the YeYo Marketplace from **{seller.seller_name}** "
                   f"(`{hero_listing.listing_id}`, listing says {hero_listing.stock_status}) and in YeYo stores. "
                   f"Clickstream streams to `{config.clickstream_topic}`, orders to `{config.pubsub_topic}`.")
        vjob: ViralJob = ss.viral_job
        v_running = bool(vjob and vjob.is_running)
        with st.expander("Demo settings"):
            c1, c2, c3, c4, c5 = st.columns(5)
            v_units = c1.number_input("Partner units on hand (last feed)", min_value=1, max_value=100000,
                                      value=config.viral_partner_units, step=50, disabled=v_running,
                                      key="viral_units")
            v_views = c2.number_input("Shoppers landing on the page", min_value=1, max_value=20000,
                                      value=config.viral_page_views, step=500, disabled=v_running, key="viral_views")
            v_secs = c3.slider("Viral window (seconds)", 0, config.spike_max_duration_seconds,
                               min(config.viral_duration_seconds, config.spike_max_duration_seconds), step=10,
                               disabled=v_running, key="viral_secs")
            v_pause = c4.number_input("Pause at partner units left (0 = no pause)", min_value=0, max_value=100000,
                                      value=config.viral_pause_units, step=50, disabled=v_running, key="viral_pause")
            v_store = c5.number_input("Units in each London store", min_value=0, max_value=100000,
                                      value=config.viral_store_units, step=50, disabled=v_running, key="viral_store")
        restocking = bool(ss.restock and ss.restock["state"] not in TERMINAL_BATCH_STATES + ("SKIPPED",))
        b1, b2, b3, _ = st.columns([1, 1, 1, 2])
        if b1.button("Go viral", type="primary", disabled=v_running or restocking):
            vcfg = ViralConfig(hero_listing.listing_id, int(v_views), float(v_secs), int(v_units),
                               pause_at_units=int(v_pause) or None,
                               store_stock=tuple((s.location_id, int(v_store)) for s in catalog.stores())).validate(
                max_duration_seconds=config.spike_max_duration_seconds)
            _clear_run()
            ss.viral_job = ViralJob(catalog, publisher, vcfg).start()
            ss.restock = None
            st.rerun()
        if b2.button("Stop", disabled=not v_running, key="viral_stop"):
            vjob.cancel()
            vjob.resume()
        if b3.button("Reset demo", disabled=v_running or restocking, key="viral_reset",
                     help="Clears the run and restocks the partner feed (Spark Iceberg, or S3 via BigQuery Omni)."):
            _reset_demo(int(v_units), int(v_store))
            st.rerun()

        @st.fragment(run_every=(5.0 if ss.restock.get("job") else 10.0) if restocking else None)
        def restock_status() -> None:
            r = ss.restock
            if not r:
                return
            if r.get("job"):
                r["state"], r["error"] = r["job"].state, r["job"].error
                if r["state"] == "SUCCEEDED":
                    st.success(f"Partner restocked on AWS: BigQuery Omni wrote {r['units']:,} units to S3 (Parquet) "
                               f"and copied Glow Lab's rows to `{config.partner_replica_table}`.")
                elif r["state"] == "FAILED":
                    st.error(f"Partner restock on AWS FAILED: {r['error']}")
                else:
                    st.info(f"Restocking partner on AWS: {r['state'].replace('_', ' ').lower()} (about 1 minute).")
                if restocking and r["state"] in TERMINAL_BATCH_STATES:
                    st.rerun(scope="app")
                return
            if r.get("name") and r["state"] not in TERMINAL_BATCH_STATES:
                try:
                    r["state"] = batch_state(r["name"])
                except Exception as exc:
                    r["error"] = f"{exc.__class__.__name__}: {str(exc)[:200]}"
            batch = r.get("name", "").rsplit("/", 1)[-1]
            if r["state"] == "SUCCEEDED":
                st.success(f"Partner restocked: Spark batch `{batch}` wrote {r['units']:,} units to the Iceberg feed.")
            elif r["state"] == "SKIPPED":
                st.info("Demo reset. Partner restock skipped (local mode or LAKEHOUSE_BUCKET not set).")
            elif r["state"] in TERMINAL_BATCH_STATES:
                st.error(f"Partner restock {r['state']}: {r.get('error') or batch}")
            else:
                st.info(f"Restocking partner: Spark batch `{batch}` is {r['state']} (about 3 minutes).")
            if restocking and r["state"] in TERMINAL_BATCH_STATES:
                st.rerun(scope="app")

        restock_status()
        polling = v_running or bool(vjob and vjob.seconds_since_end < 60)

        @st.fragment(run_every=2.0 if polling else None)
        def viral_live() -> None:
            job: ViralJob = ss.viral_job
            if job is None:
                _bids_table(PHASE_BASELINE, hero.omni_item_id)
                st.caption("Press **Go viral** to start the TikTok moment.")
                return
            snap = job.snapshot()
            _ops_bigquery(job)
            bq = ss.ops_bq
            bq_risk = (bq.get("risk") or [None])[0]
            projected = bq_risk["projected_units"] if bq_risk else snap["projected_units"]
            partner = bq_risk["partner_units"] if bq_risk else snap["partner_units"]
            level = bq_risk["risk_level"] if bq_risk else risk_level(projected, snap["cart_units"])
            ordered = bq_risk["units_ordered_since_snapshot"] if bq_risk else snap["units_ordered"]
            phase = _phase(snap)

            if snap["paused"]:
                st.warning("Spike paused for the talk track. Show the Marketing tab; coming back here resumes it.",
                           icon="⏸️")
            m = st.columns(6)
            m[0].metric("Shoppers on the page (last 60s)", f"{snap['live_shoppers']:,}")
            m[1].metric("Add-to-carts", f"{snap['add_to_cart']:,}")
            m[2].metric("Orders", f"{snap['orders']:,}")
            m[3].metric("Partner units left", f"{max(projected, 0):,}")
            m[4].metric("London store units left", f"{snap['store_units_left']:,}")
            m[5].metric("Waitlist", f"{snap['waitlist']:,}")
            if snap["projected_units"] <= 0 and snap["store_units_total"]:
                _finish_card(snap)

            if level == "OK":
                st.success(f"Partner stock OK: {max(projected, 0):,} of {partner:,} units left.")
            else:
                st.error(
                    f"OVERSELL {'RISK' if level == 'AT_RISK' else 'CAUGHT'}: listing {snap['listing_id']} says "
                    f"IN STOCK, but {snap['seller_name']}'s feed shows {partner:,} units and "
                    f"{ordered:,} have already been ordered ({max(projected, 0):,} left).",
                    icon="🚨",
                )
            st.progress(max(min(projected / partner, 1.0), 0.0) if partner else 0.0,
                        text=f"Partner stock gauge: {max(projected, 0):,} / {partner:,} units "
                             f"({'BigQuery view' if bq_risk else 'simulator'})")

            left, right = st.columns(2)
            with left:
                st.markdown("**Live: events per 10 seconds**")
                if snap["timeline"]:
                    st.line_chart({k: [t[k] for t in snap["timeline"]]
                                   for k in ("page_views", "add_to_cart", "orders", "waitlist")})
            with right:
                st.markdown("**Same orders: live vs. next-day batch report**")
                cumulative, total = [], 0
                for t in snap["timeline"]:
                    total += t["orders"]
                    cumulative.append(total)
                if cumulative:
                    st.area_chart({"Live lakehouse (minutes)": cumulative,
                                   "Next-day report (lands 06:00 tomorrow)": [0] * len(cumulative)})
                st.caption("Siloed: clickstream in Databricks on AWS, partner stock in an overnight batch, "
                           "reports in BigQuery. The trend shows up 24 hours later.")

            _bids_table(phase, snap["omni_item_id"])

            with st.expander("Proof: latest Pub/Sub events landed in BigQuery "
                             f"({config.clickstream_table})"):
                if bq.get("latest"):
                    st.dataframe(bq["latest"], hide_index=True, width="stretch")
                if bq.get("minutes"):
                    st.markdown("Clickstream per minute (landed rows)")
                    st.dataframe(bq["minutes"], hide_index=True, width="stretch")
                for key in ("risk_error", "minutes_error", "stores_error", "latest_error", "alert_error"):
                    if bq.get(key):
                        st.caption(f"BigQuery {key.split('_')[0]}: {bq[key]}")
            for err in snap["errors"]:
                st.error(err)

            if snap["pause_done"] and ss.get("surge_shown") != job.viral_id:
                ss.surge_shown = job.viral_id
                ss.open_dialog = "surge"
                st.rerun(scope="app")
            if snap["projected_units"] <= 0 and ss.get("sold_out_shown") != job.viral_id:
                ss.sold_out_shown = job.viral_id
                ss.hero_sold_out = snap["omni_item_id"]
                ss.open_dialog = "sold_out"
                st.rerun(scope="app")
            stores_out = snap["store_units_total"] and snap["store_units_left"] <= 0
            if snap["projected_units"] <= 0 and stores_out \
                    and ss.get("finale_shown") != job.viral_id and ss.get("sold_out_shown") == job.viral_id \
                    and ss.open_dialog is None:
                ss.finale_shown = job.viral_id
                ss.open_dialog = "finale"
                st.rerun(scope="app")
            if (stores_out or not job.is_running) and snap["store_units_total"] \
                    and config.publish_mode != "local" \
                    and ss.get("store_stock_synced") != (job.viral_id, snap["store_left"]):
                try:
                    reset_store_stock(config, job.product.item_number, job.product.omni_item_id,
                                      list(snap["store_left"]), snap["store_left"])
                    ss.store_stock_synced = (job.viral_id, snap["store_left"])
                except Exception as exc:
                    st.warning(f"Store stock not written to BigQuery: {exc.__class__.__name__}: {str(exc)[:200]}")
            if (not job.is_running and v_running) or (polling and not v_running and job.seconds_since_end >= 60):
                st.rerun(scope="app")

        viral_live()

    with st.expander("Demo links"):
        st.caption("Opens the Google Cloud console in a new tab (sign in to the presenting account first).")
        for scene, links in group_links(demo_links(config)).items():
            st.markdown(f"**{scene}**")
            cols = st.columns(min(len(links), 4))
            for i, link in enumerate(links):
                cols[i % len(cols)].link_button(link.label, link.url, width="stretch")
        st.code("Where can we still fulfil BEA-3001-OM? List YeYo stores with stock on hand.", language=None)


# ---------------------------------------------------------------- marketing
def _marketing_bigquery(job: ViralJob) -> None:
    if config.publish_mode == "local":
        return
    cache = ss.mkt_bq
    now = datetime.now(timezone.utc)
    if cache.get("at") and (now - cache["at"]).total_seconds() < 10:
        return
    cache["at"] = now
    for key, sql in (("referrer", by_referrer_sql(config)), ("postcode", by_postcode_sql(config))):
        try:
            cache[key] = run_query(config, sql, {"viral_id": job.viral_id})
            cache.pop(f"{key}_error", None)
        except Exception as exc:
            cache[f"{key}_error"] = f"{exc.__class__.__name__}: {str(exc)[:200]}"


with marketing_tab:
    mjob: ViralJob = ss.viral_job
    m_polling = bool(mjob and (mjob.is_running or mjob.seconds_since_end < 60))

    @st.fragment(run_every=5.0 if m_polling else None)
    def marketing_live() -> None:
        job: ViralJob = ss.viral_job
        if job is None:
            if hero_listing is not None:
                _bids_table(PHASE_BASELINE, hero_listing.omni_item_id)
            st.caption("Press **Go viral** on the Ops tab; traffic, revenue and actions for the campaign appear here.")
            return
        snap = job.snapshot()
        _marketing_bigquery(job)
        mk = ss.mkt_bq
        price = float(job.listing.listing_price)
        bq_risk = (ss.ops_bq.get("risk") or [None])[0]
        level = bq_risk["risk_level"] if bq_risk else risk_level(snap["projected_units"], snap["cart_units"])
        from_bq = bool(mk.get("referrer")) and bool(mk.get("postcode"))
        rows = ({key: mk[key] for key in ("referrer", "postcode")} if from_bq
                else simulator_rows(snap, price, level))
        t = totals(rows["referrer"])

        st.markdown(f"**{snap['description']}** on the YeYo Marketplace ({snap['seller_name']}), "
                    f"campaign `{snap['viral_id']}`")
        k = st.columns(5)
        k[0].metric("Sessions", f"{t['sessions']:,.0f}")
        k[1].metric("From TikTok", f"{t['tiktok_share']:.0%}")
        k[2].metric("Conversion", f"{t['conversion']:.1%}")
        k[3].metric("Revenue", _money(t["revenue"]))
        k[4].metric("Revenue at risk (waitlist)", _money(t["revenue_at_risk"]))

        if level == "OK":
            st.success("Demand is converting and partner stock is OK: scale TikTok and Shopping spend while "
                       "CPCs are still low.")
        else:
            sub = _substitute_product(snap["omni_item_id"])
            st.error(f"Pause ads that send shoppers to listing {snap['listing_id']}: the partner is "
                     f"{'OVERSOLD' if level == 'OVERSOLD' else 'about to oversell'}. Geo-target "
                     f"\"collect in store today\" where a store has stock"
                     + (f", and switch the rest of the budget to **{sub.description}**." if sub else "."),
                     icon="📣")

        left, right = st.columns(2)
        with left:
            st.markdown("**Funnel**")
            st.bar_chart(
                {"stage": ["1 Page views", "2 Added to cart", "3 Purchased", "4 Waitlisted"],
                 "units": [t["page_views"], t["cart_units"], t["purchase_units"], t["waitlist_units"]]},
                x="stage", y="units", horizontal=True)
        with right:
            st.markdown("**Sessions by traffic source**")
            if rows["referrer"]:
                st.bar_chart({"referrer": [r["referrer"] for r in rows["referrer"]],
                              "sessions": [float(r["sessions"]) for r in rows["referrer"]]},
                             x="referrer", y="sessions", horizontal=True)

        _bids_table(_phase(snap), snap["omni_item_id"])

        st.markdown("**Demand by postcode and next best action**")
        st.dataframe(
            [{"Postcode": r["postal_code"], "Sessions": r["sessions"], "Conversion": f"{float(r['conversion'] or 0):.1%}",
              "Revenue": _money(r["revenue"]), "Revenue at risk": _money(r["revenue_at_risk"]),
              "Nearest store with stock": r["nearest_store"] or "-",
              "Next best action": r["next_best_action"]} for r in rows["postcode"]],
            hide_index=True, width="stretch")
        st.caption(f"Source: {'BigQuery ' + config.marketing_view if from_bq else 'simulator (BigQuery not available)'} "
                   f"(clickstream landed through Pub/Sub, oversell view, stock by store).")
        for key in ("referrer_error", "postcode_error"):
            if mk.get(key):
                st.caption(f"BigQuery {key.split('_')[0]}: {mk[key]}")

    marketing_live()

# ---------------------------------------------------------------- data agent
AGENT_QUESTIONS = (
    "Where can we still fulfil BEA-3001-OM? List YeYo stores with stock on hand from stock_master_location "
    "for the latest stock_date.",
    "How many units of BEA-3001-OM were sold by channel today, and how many were click & collect by store?",
    "Which products have the most marketplace orders in the last hour?",
)


def _ask_agent(question: str) -> None:
    chat = ss.setdefault("agent_chat", [])
    chat.append({"role": "user", "text": question})
    try:
        if not ss.get("agent_session"):
            ss.agent_user = data_agent.new_user_id()
            ss.agent_session = data_agent.create_session(config.data_agent_url, config.data_agent_app, ss.agent_user)
        answer, steps = data_agent.ask(config.data_agent_url, config.data_agent_app, ss.agent_user,
                                       ss.agent_session, question)
        chat.append({"role": "assistant", "text": answer or "(no answer)", "steps": steps})
    except Exception as exc:
        chat.append({"role": "assistant", "text": f"Agent call failed: {exc.__class__.__name__}: {str(exc)[:300]}"})


with agent_tab:
    st.markdown("### Ask the data analyst agent")
    st.caption(f"ADK agent `{config.data_agent_app}`: Gemini writes the SQL, the agent runs it in BigQuery "
               f"(`{config.bq_dataset}`) and answers with the query it used.")
    if not config.data_agent_url:
        st.info("Set `DATA_AGENT_URL` to the agent's ADK API server: `adk api_server` locally "
                "(http://localhost:8000) or the `yeyo-data-agent` Cloud Run URL (Module 13).")
    else:
        cols = st.columns(len(AGENT_QUESTIONS))
        for i, q in enumerate(AGENT_QUESTIONS):
            if cols[i].button(q, key=f"agent_q{i}", width="stretch"):
                with st.spinner("The agent is writing and running SQL..."):
                    _ask_agent(q)
        for msg in ss.get("agent_chat", []):
            with st.chat_message(msg["role"]):
                st.markdown(msg["text"])
                for step in msg.get("steps") or []:
                    with st.expander(f"Tool: {step['tool']}"):
                        st.json({k: step.get(k) for k in ("args", "result")}, expanded=False)
        question = st.chat_input("Ask about orders, stock, customers...", key="agent_input")
        if question:
            with st.spinner("The agent is writing and running SQL..."):
                _ask_agent(question)
            st.rerun()
        if ss.get("agent_chat") and st.button("New conversation", key="agent_reset"):
            ss.agent_chat, ss.agent_session = [], None
            st.rerun()

# ---------------------------------------------------------------- story pop-ups
def _close_dialog() -> None:
    ss.open_dialog = None


@st.dialog("Viral alert", width="large", on_dismiss=_close_dialog)
def _surge_dialog() -> None:
    snap = ss.viral_job.snapshot()
    left = max(snap["projected_units"], 0)
    rate = snap["units_ordered"] / snap["elapsed_seconds"] if snap["elapsed_seconds"] else 0
    st.markdown(f"### {snap['description']} is going viral on TikTok")
    st.markdown(
        f"**{snap['live_shoppers']:,} shoppers** landed on the product page in the last minute and "
        f"**{snap['add_to_cart']:,}** have added it to their carts. {snap['seller_name']} has only "
        f"**{left:,} units** left" + (f", gone in about **{left / rate / 60:.0f} minutes** at this rate." if rate else ".")
    )
    st.markdown("A retailer who sees this report a day later has already missed it. Instead, a modern retailer "
                "with the GCP open lakehouse and BigQuery capitalises on this trend dynamically. Their marketing "
                "team reacts now: high-bid Google Shopping and social ads capture the search traffic around the "
                "video before competitors do.")
    if st.button("Deploy high-bid Google Shopping + social ads", type="primary"):
        _close_dialog()
        st.rerun()


@st.dialog("Sold out on the Marketplace", width="large", on_dismiss=_close_dialog)
def _sold_out_dialog() -> None:
    snap = ss.viral_job.snapshot()
    st.markdown(f"### {snap['description']} can now only be collected in store")
    st.markdown(f"{snap['seller_name']}'s {snap['partner_units']:,} units are gone. Without the lakehouse the "
                "spike would end here, with the London stores still holding stock. Instead, Marketplace ads are "
                "paused and local inventory ads send shoppers to click & collect from YeYo stores:")
    st.markdown("\n".join(f"- {_store_name(s)}" + (f": {q:,} units" if q else "") for s, q in _stores_with_stock()))
    sub = _substitute_product(snap["omni_item_id"])
    if sub is not None:
        st.markdown(f"Marketing also launches high-bid ads for our private label **{sub.description}** to capture "
                    "the rest of the search and social traffic around the video.")
    if st.button("OK", type="primary"):
        _close_dialog()
        st.rerun()


@st.dialog("The finish: store stock sold out too", width="large", on_dismiss=_close_dialog)
def _finale_dialog() -> None:
    snap = ss.viral_job.snapshot()
    partner, stores = snap["units_ordered"], snap["store_units_sold"]
    st.markdown(f"### {partner + stores:,} units sold instead of {partner:,}")
    st.markdown(f"- **Without the lakehouse:** {partner:,} units on the Marketplace, then empty shelves online. "
                "The trend shows up in tomorrow's report; store stock is never used.")
    st.markdown(f"- **With BigQuery and the open lakehouse:** {partner:,} on the Marketplace plus **{stores:,} click & "
                "collect** from London stores, footfall driven by local inventory ads"
                + (f": **{(partner + stores) / partner:.1f}× the units**." if partner else "."))
    st.markdown("Same viral moment, same product. The only difference is whether your data arrives in minutes "
                "or tomorrow, and that's an architecture decision.")
    if st.button("OK", type="primary"):
        _close_dialog()
        st.rerun()


if ss.viral_job is not None and ss.open_dialog == "surge":
    _surge_dialog()
elif ss.viral_job is not None and ss.open_dialog == "sold_out":
    _sold_out_dialog()
elif ss.viral_job is not None and ss.open_dialog == "finale":
    _finale_dialog()
