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
from storefront.landing import landing_sql, query_landing
from storefront.publisher import AutoPublisher, BasePublisher, create_publisher
from storefront.ops import (
    demo_links,
    group_links,
    latest_clickstream_sql,
    oversell_sql,
    per_minute_sql,
    record_alert_sql,
    risk_level,
    run_query,
    store_stock_sql,
)
from storefront.spike import SpikeConfig, SpikeJob
from storefront.ops import TERMINAL_BATCH_STATES, batch_state, submit_restock
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
ss.setdefault("spike_job", None)
ss.setdefault("viral_job", None)
ss.setdefault("restock", None)
ss.setdefault("ops_bq", {})
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
    try:
        event = build_order_event(
            catalog,
            _cart_lines(),
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

shop_tab, spike_tab, ops_tab, log_tab = st.tabs(
    ["Shop", "Simulate order spike", "Ops: viral moment", "Orders & BigQuery landing"])

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

# ---------------------------------------------------------------- spike
with spike_tab:
    st.markdown(
        "Generate a burst of synthetic **online** and **marketplace** orders and publish each one to "
        f"the same topic as storefront orders (`{config.pubsub_topic}`). Runs in the background."
    )
    job: SpikeJob = ss.spike_job
    running = bool(job and job.is_running)

    c1, c2, c3 = st.columns(3)
    num_orders = c1.number_input("Number of orders", min_value=1, max_value=config.spike_max_orders,
                                 value=config.spike_default_orders, step=50, disabled=running,
                                 key="spike_orders")
    duration = c2.slider("Burst window (seconds)", 0, config.spike_max_duration_seconds,
                         config.spike_default_duration_seconds, step=5, disabled=running,
                         help="0 = publish as fast as possible", key="spike_duration")
    mkt_share = c3.slider("Marketplace share (%)", 0, 100, 40, step=5, disabled=running,
                          key="spike_mkt_share")
    st.caption(f"Max burst size: {config.spike_max_orders} orders / {config.spike_max_duration_seconds}s "
               "(set SPIKE_MAX_ORDERS / SPIKE_MAX_DURATION_SECONDS).")

    b1, b2, _ = st.columns([1, 1, 3])
    if b1.button("Simulate order spike", type="primary", disabled=running):
        spike_cfg = SpikeConfig(int(num_orders), float(duration), mkt_share / 100).validate(
            config.spike_max_orders, config.spike_max_duration_seconds)
        ss.spike_job = SpikeJob(catalog, publisher, spike_cfg).start()
        st.rerun()
    if b2.button("Stop", disabled=not running):
        job.cancel()

    @st.fragment(run_every=1.0 if running else None)
    def spike_progress() -> None:
        job: SpikeJob = ss.spike_job
        if job is None:
            st.caption("No spike run yet.")
            return
        snap = job.snapshot()
        done = snap["published"] + snap["failed"]
        st.progress(min(done / snap["total"], 1.0),
                    text=f"Published {snap['published']}/{snap['total']} events"
                         + (f" - {snap['failed']} failed" if snap["failed"] else ""))
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Status", snap["status"])
        m2.metric("Online", snap["by_channel"].get(CHANNEL_ONLINE, 0))
        m3.metric("Marketplace", snap["by_channel"].get(CHANNEL_MARKETPLACE, 0))
        m4.metric("Events/sec", snap["events_per_second"])
        for err in snap["errors"]:
            st.error(err)
        if not job.is_running:
            st.success(
                f"Spike {snap['spike_id']} {snap['status']}: {snap['published']} published, "
                f"{snap['failed']} failed in {snap['elapsed_seconds']}s - "
                f"{_money(snap['order_total'])} simulated revenue. Filter BigQuery on spike_id = '{snap['spike_id']}'."
            )
            if running:
                st.rerun(scope="app")
        if snap["recent_events"]:
            with st.expander("Latest spike events"):
                st.dataframe(
                    [{k: e[k] for k in ("order_id", "order_ts", "channel", "marketplace_seller_id",
                                        "item_count", "order_total")} for e in reversed(snap["recent_events"])],
                    hide_index=True, width="stretch",
                )

    spike_progress()

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


def _reset_demo(units: int) -> None:
    """Clear the viral run and restock the partner with a fresh Spark-written Iceberg snapshot."""
    ss.viral_job = None
    ss.ops_bq = {}
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
        st.markdown(
            f"**{hero.description}** (`{hero.omni_item_id}`) from marketplace partner **{seller.seller_name}** "
            f"(`{hero_listing.listing_id}`, listing says **{hero_listing.stock_status}**). "
            f"Clickstream goes to `{config.clickstream_topic}`, orders to `{config.pubsub_topic}`."
        )
        vjob: ViralJob = ss.viral_job
        v_running = bool(vjob and vjob.is_running)
        c1, c2, c3 = st.columns(3)
        v_units = c1.number_input("Partner units on hand (last feed)", min_value=1, max_value=100000,
                                  value=config.viral_partner_units, step=50, disabled=v_running, key="viral_units")
        v_views = c2.number_input("Shoppers landing on the page", min_value=1, max_value=20000,
                                  value=config.viral_page_views, step=500, disabled=v_running, key="viral_views")
        v_secs = c3.slider("Viral window (seconds)", 0, config.spike_max_duration_seconds,
                           min(config.viral_duration_seconds, config.spike_max_duration_seconds), step=10,
                           disabled=v_running, key="viral_secs")
        restocking = bool(ss.restock and ss.restock["state"] not in TERMINAL_BATCH_STATES + ("SKIPPED",))
        b1, b2, b3, _ = st.columns([1, 1, 1, 2])
        if b1.button("Go viral", type="primary", disabled=v_running or restocking):
            vcfg = ViralConfig(hero_listing.listing_id, int(v_views), float(v_secs), int(v_units)).validate(
                max_duration_seconds=config.spike_max_duration_seconds)
            ss.viral_job = ViralJob(catalog, publisher, vcfg).start()
            ss.ops_bq = {}
            ss.restock = None
            st.rerun()
        if b2.button("Stop", disabled=not v_running, key="viral_stop"):
            vjob.cancel()
        if b3.button("Reset demo", disabled=v_running or restocking, key="viral_reset",
                     help="Clears the run and restocks the partner: a Spark batch writes a fresh Iceberg snapshot."):
            _reset_demo(int(v_units))
            st.rerun()

        @st.fragment(run_every=10.0 if restocking else None)
        def restock_status() -> None:
            r = ss.restock
            if not r:
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
                st.caption("Press **Go viral** to start the TikTok moment.")
                return
            snap = job.snapshot()
            _ops_bigquery(job)
            bq = ss.ops_bq
            bq_risk = (bq.get("risk") or [None])[0]
            projected = bq_risk["projected_units"] if bq_risk else snap["projected_units"]
            partner = bq_risk["partner_units"] if bq_risk else snap["partner_units"]
            level = bq_risk["risk_level"] if bq_risk else risk_level(projected, snap["cart_units"])

            m = st.columns(5)
            m[0].metric("Page views", f"{snap['page_views']:,}")
            m[1].metric("Add-to-carts", f"{snap['add_to_cart']:,}")
            m[2].metric("Marketplace orders", f"{snap['orders']:,}")
            m[3].metric("Waitlist", f"{snap['waitlist']:,}")
            m[4].metric("Status", snap["status"])

            if level == "OK":
                st.success(f"Partner stock OK: {max(projected, 0):,} of {partner:,} units left.")
            else:
                st.error(
                    f"OVERSELL {'RISK' if level == 'AT_RISK' else 'CAUGHT'}: listing {snap['listing_id']} says "
                    f"IN STOCK, but {snap['seller_name']}'s feed shows {partner:,} units and "
                    f"{snap['units_ordered']:,} have already been ordered ({max(projected, 0):,} left).",
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

            with st.container(border=True):
                st.markdown("**Simulated marketing response**")
                stores = [r for r in (bq.get("stores") or []) if r["location_id"].endswith("-ST")
                          and r["quantity_on_hand"] > 0]
                sub = _substitute_product(snap["omni_item_id"])
                c_a, c_b = st.columns(2)
                c_a.markdown(
                    "- Keywords: *tiktok eye tightener*, *instant eye firming*, *viral under eye cream*\n"
                    "- Channels: Google Shopping + social, high bid while CPCs are still low\n"
                    f"- Waitlist: **on** for late visitors ({snap['waitlist']:,} joined)"
                )
                if stores:
                    c_b.markdown("Collect in store today:\n" + "\n".join(
                        f"- {r['location_id']}: {r['quantity_on_hand']} units" for r in stores))
                else:
                    c_b.caption("Branch stock appears here from BigQuery (stock_master_location).")
                if projected <= 0 and sub is not None:
                    st.warning(f"Partner stock is gone: budget moves to **{sub.description}** (`{sub.omni_item_id}`).")
            for key in ("risk_error", "minutes_error", "stores_error", "latest_error", "alert_error"):
                if bq.get(key):
                    st.caption(f"BigQuery {key.split('_')[0]}: {bq[key]}")
            if bq.get("minutes"):
                with st.expander("BigQuery clickstream per minute (landed rows)"):
                    st.dataframe(bq["minutes"], hide_index=True, width="stretch")
            if bq.get("latest"):
                with st.expander(f"Latest Pub/Sub events landed in BigQuery ({config.clickstream_table})",
                                 expanded=True):
                    st.dataframe(bq["latest"], hide_index=True, width="stretch")
            for err in snap["errors"]:
                st.error(err)
            if (not job.is_running and v_running) or (polling and not v_running and job.seconds_since_end >= 60):
                st.rerun(scope="app")

        viral_live()

    st.subheader("Demo links")
    st.caption("Opens the Google Cloud console in a new tab (sign in to the presenting account first).")
    for scene, links in group_links(demo_links(config)).items():
        st.markdown(f"**{scene}**")
        cols = st.columns(min(len(links), 4))
        for i, link in enumerate(links):
            cols[i % len(cols)].link_button(link.label, link.url, width="stretch")
    st.code("Where can we still fulfil BEA-3001-OM? List YeYo stores with stock on hand.", language=None)

# ---------------------------------------------------------------- log
with log_tab:
    st.subheader("Orders placed in this session")
    if ss.orders:
        st.dataframe(
            [{"order_id": e["order_id"], "order_ts": e["order_ts"], "channel": e["channel"],
              "customer_id": e["customer_id"], "marketplace_seller_id": e["marketplace_seller_id"],
              "items": e["item_count"], "order_total": e["order_total"]} for e in ss.orders],
            hide_index=True, width="stretch",
        )
    else:
        st.caption("Place an order on the Shop tab.")

    st.subheader("BigQuery landing (last 30 minutes, rows per minute)")
    if config.publish_mode == "local":
        st.caption("PUBLISH_MODE=local - events go to a local file, not BigQuery.")
    elif st.button("Refresh from BigQuery"):
        try:
            rows = query_landing(config)
            st.dataframe(rows, hide_index=True, width="stretch") if rows else st.info(
                "No rows yet - BigQuery subscriptions usually land rows within seconds.")
            ss.landing_checked_at = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        except Exception as exc:
            st.error(f"BigQuery query failed: {exc.__class__.__name__}: {exc}")
    with st.expander("SQL"):
        st.code(landing_sql(config), language="sql")
