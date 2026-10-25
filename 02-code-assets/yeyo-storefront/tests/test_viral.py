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

import re
from types import SimpleNamespace
from pathlib import Path

from storefront.catalog import SAMPLE_DATA_DIR, _read_csv, load_bigquery_catalog
from storefront.ops import CONSOLE, demo_links, group_links, restock_batch, risk_level, submit_restock
from storefront.publisher import BigQueryPublisher, PubSubPublisher, event_attributes, event_to_row
from storefront.marketing import (PHASE_SOLD_OUT, PHASE_STORES_OUT, PHASE_SURGE, PHASE_VIRAL, campaign_bids, story_phase,
                                  ACTION_SCALE, ACTION_STORE, ACTION_SUBSTITUTE, by_postcode_sql,
                                  by_referrer_sql, next_best_action, simulator_rows, totals)
from storefront.viral import ViralConfig, ViralJob, build_clickstream_event, viral_listing
from test_publisher_and_spike import FakeBigQueryClient, FakePubSubClient

ROOT = Path(__file__).resolve().parent.parent


def _run(catalog, publisher, **kw):
    cfg = ViralConfig(**{"page_views": 400, "duration_seconds": 0, "partner_units": 40, "seed": 7, **kw})
    job = ViralJob(catalog, publisher, cfg.validate()).start()
    job.join(30)
    return job


def test_viral_routes_clickstream_and_orders_to_their_topics(catalog, config):
    client = FakePubSubClient()
    job = _run(catalog, PubSubPublisher(config, client=client))
    snap = job.snapshot()
    assert snap["status"] == "done" and snap["failed"] == 0
    by_topic = {}
    for topic, msg, attrs in client.messages:
        by_topic.setdefault(topic, []).append((msg, attrs))
    orders = by_topic[config.topic_path]
    clicks = by_topic[config.clickstream_topic_path]
    assert len(orders) == snap["orders"] > 0
    assert len(clicks) == snap["page_views"] + snap["add_to_cart"] + snap["waitlist"] + snap["purchases"]
    assert snap["purchases"] == snap["orders"]
    assert snap["page_views"] == 400
    for msg, attrs in orders:
        assert attrs["channel"] == "marketplace" and msg["event_source"] == "viral_simulator"
        assert msg["spike_id"] == job.viral_id
        (line,) = msg["line_items"]
        assert (line["omni_item_id"], line["marketplace_listing_id"], line["marketplace_seller_id"]) == (
            "BEA-3001-OM", "LST-0017", "MKT-S004")
    assert {m["omni_item_id"] for m, _ in clicks} == {"BEA-3001-OM"}
    assert {m["listing_stock_status"] for m, _ in clicks} == {"IN_STOCK"}


def test_viral_stops_selling_at_partner_stock_and_fills_waitlist(catalog, config):
    job = _run(catalog, PubSubPublisher(config, client=FakePubSubClient()))
    snap = job.snapshot()
    assert config.viral_partner_units > 0
    assert 40 <= snap["units_ordered"] <= 41
    assert snap["projected_units"] <= 0 and snap["waitlist"] > 0


def test_viral_curve_rises(catalog, config):
    job = ViralJob(catalog, PubSubPublisher(config, client=FakePubSubClient()),
                   ViralConfig(page_views=2000, duration_seconds=100, seed=1))
    first_half = sum(1 for t in job._schedule if t < 50)
    assert first_half < 2000 - first_half


def test_bigquery_publisher_routes_clickstream_table(catalog, config):
    bq = FakeBigQueryClient()
    _run(catalog, BigQueryPublisher(config, client=bq), page_views=50)
    assert set(bq.rows) == {config.fq_table("clickstream_events"), config.fq_table("marketplace_orders")}


def test_clickstream_schema_matches_viral_sql(catalog):
    import random

    ev = build_clickstream_event(catalog, viral_listing(catalog, "LST-0017"), "page_view", random.Random(1))
    assert event_attributes(ev)["channel"] == "marketplace"
    body = (ROOT / "sql" / "viral_moment.sql").read_text()
    body = body.split("CREATE TABLE IF NOT EXISTS capstone_ds.clickstream_events (", 1)[1].split("\n)\n", 1)[0]
    cols = set(re.findall(r"^  (\w+) ", body, flags=re.M))
    assert set(event_to_row(ev)) == cols


def test_risk_level_matches_view_rule():
    assert risk_level(100, 50) == "OK"
    assert risk_level(100, 150) == "AT_RISK"
    assert risk_level(0, 0) == "OVERSOLD"


def test_demo_links_use_config(config, monkeypatch):
    links = demo_links(config)
    assert all("project=test-project" in link.url for link in links if link.url.startswith(CONSOLE))
    assert not any("storage/browser" in link.url for link in links)
    assert "3c · Engineering: Spark + Iceberg" in group_links(links)
    bucketed = demo_links(config.__class__(**{**config.__dict__, "lakehouse_bucket": "b1", "agent_url": "http://a"}))
    assert any("storage/browser/b1/iceberg" in link.url for link in bucketed)
    assert bucketed[-1].url == "http://a"


def test_looker_link_targets_oversell_view(config):
    from urllib.parse import parse_qs, urlparse
    looker = [link for link in demo_links(config) if "Looker" in link.label]
    assert len(looker) == 1 and looker[0].scene == "3a · Oversell alert"
    qs = {k: v[0] for k, v in parse_qs(urlparse(looker[0].url).query).items()}
    assert qs["ds.connector"] == "bigQuery" and qs["ds.type"] == "TABLE"
    assert (qs["ds.projectId"], qs["ds.datasetId"], qs["ds.tableId"]) == ("test-project", config.bq_dataset,
                                                                          config.oversell_view)
    saved = demo_links(config.__class__(**{**config.__dict__, "looker_report_url": "https://lookerstudio.google.com/r"}))
    assert [link.url for link in saved if "Looker" in link.label] == ["https://lookerstudio.google.com/r"]


def test_no_alloydb_dependency():
    sources = list((ROOT / "storefront").glob("*.py")) + [ROOT / "app.py", ROOT / "requirements.txt"]
    assert not any("alloydb" in p.read_text().lower() for p in sources)


class _Future:
    def __init__(self, exc=None):
        self._exc = exc

    def result(self, timeout=None):
        if self._exc:
            raise self._exc
        return "ok"


class _FailOrdersPublisher:
    def publish_many(self, events):
        return [_Future(RuntimeError("boom") if e["event_type"] == "order_placed" else None) for e in events]

    def flush(self):
        pass


def test_failed_orders_do_not_consume_partner_stock(catalog):
    job = _run(catalog, _FailOrdersPublisher())
    snap = job.snapshot()
    assert snap["orders"] > 0 and job.failed == snap["orders"]
    assert snap["units_ordered"] == 0 and snap["waitlist"] == 0


def test_bigquery_catalog_keeps_bundled_and_synthesized_listings(config):
    rows = _read_csv(SAMPLE_DATA_DIR / "product_master.csv")
    extra = {k: v.replace("BEA-3001", "BEA-9999") for k, v in next(r for r in rows if r["item_number"] == "BEA-3001").items()}

    class Client:
        def query(self, sql):
            if "product_master" in sql:
                return SimpleNamespace(result=lambda: rows + [extra])
            raise RuntimeError("not found")

    cat = load_bigquery_catalog(config, client=Client())
    ids = {(l.listing_id, l.omni_item_id) for l in cat.listings}
    assert ("LST-0017", "BEA-3001-OM") in ids
    assert any(omni == "BEA-9999-OM" for _, omni in ids)


def test_restock_batch_and_submit(config):
    cfg = config.__class__(**{**config.__dict__, "lakehouse_bucket": "b1"})
    batch_id, body = restock_batch(cfg, 600, batch_id="glowlab-restock-1")
    py = body["pysparkBatch"]
    assert py["mainPythonFileUri"] == "gs://b1/spark/partner_stock_sync.py"
    assert py["args"][:2] == ["--units", "600"] and len(py["jarFileUris"]) == 2
    props = body["runtimeConfig"]["properties"]
    assert props["spark.sql.catalog.yeyo_lake.warehouse"] == "bl://projects/test-project/catalogs/yeyo_lake"
    assert "environmentConfig" not in body

    calls = []

    class Session:
        def post(self, url, params, json, timeout):
            calls.append((url, params, json))
            return SimpleNamespace(raise_for_status=lambda: None)

    name = submit_restock(cfg, 600, session=Session())
    assert calls[0][0].endswith(f"/projects/test-project/locations/{cfg.location}/batches")
    assert name.endswith(calls[0][1]["batchId"])


class _TimeoutOrdersPublisher(_FailOrdersPublisher):
    def publish_many(self, events):
        return [_Future(TimeoutError() if e["event_type"] == "order_placed" else None) for e in events]


def test_timed_out_orders_keep_partner_stock_consumed(catalog):
    job = _run(catalog, _TimeoutOrdersPublisher())
    assert job.snapshot()["units_ordered"] > 0


def test_console_links_use_current_paths(config):
    urls = [link.url for link in demo_links(config)]
    assert any("/dataplex/dp-search?" in u for u in urls)
    assert any("/dataplex/govern/data-profiling-and-quality?" in u for u in urls)
    assert any("/biglake?" in u for u in urls)
    assert not any("/dataplex/search?" in u or "/bigquery/lakehouse" in u for u in urls)


def test_session_events_share_referrer_and_postcode(catalog, config):
    client = FakePubSubClient()
    _run(catalog, PubSubPublisher(config, client=client))
    sessions = {}
    for topic, msg, _ in client.messages:
        if topic == config.clickstream_topic_path:
            sessions.setdefault(msg["session_id"], set()).add((msg["referrer"], msg["postal_code"]))
    assert sessions and all(len(v) == 1 for v in sessions.values())


def test_marketing_rows_from_simulator(catalog, config):
    job = _run(catalog, PubSubPublisher(config, client=FakePubSubClient()))
    snap = job.snapshot()
    rows = simulator_rows(snap, 10.0, "OVERSOLD")
    t = totals(rows["referrer"])
    assert t["sessions"] == snap["page_views"] == totals(rows["postcode"])["sessions"]
    assert t["purchase_units"] == snap["units_ordered"] and t["revenue"] == snap["units_ordered"] * 10.0
    assert t["waitlist_units"] > 0 and 0 < t["tiktok_share"] < 1
    assert {r["next_best_action"] for r in rows["postcode"]} == {ACTION_SUBSTITUTE}


def test_next_best_action_matches_view():
    assert next_best_action("OK", "Chicago store") == ACTION_SCALE
    assert next_best_action("OVERSOLD", "Chicago store") == ACTION_STORE + "Chicago store"
    assert next_best_action("AT_RISK", None) == ACTION_SUBSTITUTE
    sql = (ROOT / "sql" / "viral_moment.sql").read_text()
    for text in (ACTION_SCALE, ACTION_STORE, ACTION_SUBSTITUTE):
        assert f"'{text}" in sql


def test_marketing_sql_targets_view(config):
    for sql in (by_referrer_sql(config), by_postcode_sql(config)):
        assert "`test-project.capstone_ds.vw_marketing_viral`" in sql and "@viral_id" in sql


def test_viral_pauses_at_threshold_until_resumed(catalog, config):
    cfg = ViralConfig(page_views=400, duration_seconds=0, partner_units=40, seed=7, pause_at_units=20)
    job = ViralJob(catalog, PubSubPublisher(config, client=FakePubSubClient()), cfg).start()
    for _ in range(100):
        if job.is_paused:
            break
        job.join(0.05)
    snap = job.snapshot()
    assert snap["paused"] and job.is_running and 18 <= snap["projected_units"] <= 20
    job.join(0.3)
    assert job.snapshot()["page_views"] == snap["page_views"]
    job.resume()
    job.join(30)
    assert not job.is_running and job.projected_units <= 0 and job.snapshot()["pause_done"]


def test_campaign_bids_follow_story():
    others = [("Allergy relief", "allergy")]
    assert story_phase(True, False, 300) == PHASE_VIRAL
    assert story_phase(True, True, 300) == PHASE_SURGE
    assert story_phase(True, True, 0) == PHASE_SOLD_OUT
    assert [r["Product"] for r in campaign_bids(PHASE_VIRAL, "Hero", others)] == ["Allergy relief"]
    surge = campaign_bids(PHASE_SURGE, "Hero", others)
    assert sum(r["Product"] == "Hero" and r["Status"].startswith("NEW") for r in surge) == 3
    sold = campaign_bids(PHASE_SOLD_OUT, "Hero", others, "YeYo serum", ["Chicago store"])
    assert sold[0]["Campaign"] == "Collect in store today" and "Chicago store" in sold[0]["Targeting"]
    assert sold[1]["Campaign"] == "Private label" and sold[1]["Product"] == "YeYo serum"
    assert sold[0]["Status"].startswith("NEW") and sold[1]["Status"].startswith("NEW")
    assert all(r["Status"].startswith("Paused") for r in sold if r["Campaign"].startswith("Viral"))
    assert story_phase(True, True, 0, 10) == PHASE_SOLD_OUT and story_phase(True, True, 0, 0) == PHASE_STORES_OUT
    done = campaign_bids(PHASE_STORES_OUT, "Hero", others, "YeYo serum", ["Chicago store"])
    assert done[0]["Status"] == "Paused: stores sold out" and done[1]["Status"].startswith("NEW")


def test_viral_continues_as_click_and_collect_from_store_stock(catalog, config):
    stores = [s.location_id for s in catalog.stores()]
    cfg = ViralConfig(page_views=3000, duration_seconds=0, partner_units=20, seed=3,
                      store_stock=tuple((s, 15) for s in stores)).validate()
    client = FakePubSubClient()
    job = ViralJob(catalog, PubSubPublisher(config, client=client), cfg).start()
    job.join(30)
    snap = job.snapshot()
    assert snap["units_ordered"] == 20 and snap["store_units_sold"] == 15 * len(stores) == snap["store_units_total"]
    assert snap["store_units_left"] == 0 and snap["waitlist"] > 0
    assert snap["units_sold_total"] == 20 + 15 * len(stores)
    orders = [data for _, data, _ in client.messages if data.get("line_items")]
    collect = [o for o in orders if o["fulfillment_type"] == "CLICK_AND_COLLECT"]
    assert collect and all(o["location_id"] in stores and o["channel"] == "online" for o in collect)
    assert sum(li["quantity"] for o in collect for li in o["line_items"]) == 15 * len(stores)


def test_store_stock_reset_sql_replaces_todays_rows(config):
    from storefront.ops import store_stock_reset_sql
    sql = store_stock_reset_sql(config)
    assert "DELETE FROM" in sql and "INSERT INTO" in sql and "UNNEST(@locations)" in sql
    assert config.stock_location_table in sql and "@units[OFFSET(i)]" in sql


def test_reset_store_stock_sends_per_store_units(config):
    class FakeBq:
        def query(self, sql, job_config=None):
            self.params = {p.name: p.values for p in job_config.query_parameters if hasattr(p, "values")}
            return self

        def result(self):
            return []

    from storefront.ops import reset_store_stock
    bq = FakeBq()
    reset_store_stock(config, "BEA-3001", "BEA-3001-OM", ["A", "B"], {"A": 0, "B": 12}, client=bq)
    assert bq.params == {"units": [0, 12], "locations": ["A", "B"]}
    reset_store_stock(config, "BEA-3001", "BEA-3001-OM", ["A", "B"], 400, client=bq)
    assert bq.params["units"] == [400, 400]


def _aws_config(config, **kw):
    return config.__class__(**{**config.__dict__, "aws_s3_bucket": "yeyo-glowlab-1", **kw})


def test_aws_demo_links_add_scene_next_to_spark(config):
    assert "3d · Engineering: AWS S3 + BigQuery Omni" not in group_links(demo_links(config))
    links = demo_links(_aws_config(config, agent_url="http://a"))
    grouped = group_links(links)
    assert "3c · Engineering: Spark + Iceberg" in grouped
    aws = grouped["3d · Engineering: AWS S3 + BigQuery Omni"]
    assert any(link.url.endswith("!2syeyo_partner_aws!3spartner_stock_iceberg") for link in aws)
    assert any(link.url.endswith("!2syeyo_partner_aws!3spartner_stock_s3") for link in aws)
    assert aws[-1].url == "https://s3.console.aws.amazon.com/s3/buckets/yeyo-glowlab-1?region=us-east-1"
    assert links[-1].url == "http://a"


def test_aws_restock_sql_and_steps(config):
    from storefront.ops import AwsRestock, PartnerListing, aws_restock_enabled, omni_stage_sql, s3_export_sql
    cfg = _aws_config(config)
    assert aws_restock_enabled(cfg) and not aws_restock_enabled(config)
    assert cfg.aws_location == "aws-us-east-1"
    export = s3_export_sql(cfg)
    assert "WITH CONNECTION `test-project.aws-us-east-1.yeyo_aws_partner`" in export
    assert "uri = 's3://yeyo-glowlab-1/partner_stock/snapshot/*.parquet'" in export and "UNNEST(@rows)" in export
    stage = omni_stage_sql(cfg)
    assert "`test-project.yeyo_partner_east4.partner_stock`" in stage
    assert "FROM `test-project.yeyo_partner_aws.partner_stock_s3`" in stage and "@seller_id" in stage

    calls = []

    class Client:
        def __init__(self, location):
            self.location = location

        def query(self, sql, job_config):
            calls.append((self.location, "query", sql, job_config.query_parameters))
            return SimpleNamespace(result=lambda: None)

        def copy_table(self, src, dst, job_config):
            calls.append((self.location, "copy", src, dst))
            return SimpleNamespace(result=lambda: None)

    listings = [PartnerListing("LST-0020", "MKT-S003", "LIF-4001-OM", "LIF-4001"),
                PartnerListing("LST-0024", "MKT-S003", "LIF-4005-OM", "LIF-4005")]
    job = AwsRestock(cfg, listings, {"LST-0020": 1500, "LST-0024": 40}, client_factory=Client).run()
    assert (job.state, job.error) == ("SUCCEEDED", "")
    assert [(c[0], c[1]) for c in calls] == [("aws-us-east-1", "query"), ("aws-us-east-1", "query"),
                                             ("us-east4", "copy")]
    rows = calls[0][3][0].values
    assert [(r.struct_values["listing_id"], r.struct_values["units_on_hand"]) for r in rows] == [
        ("LST-0020", 1500), ("LST-0024", 40)]
    assert calls[0][3][1].value == "FITGEAR-INDIANAPOLIS-DC" and "@warehouse_id" in export
    assert calls[1][3][0].value == "MKT-S003"
    assert calls[2][2:] == ("test-project.yeyo_partner_east4.partner_stock",
                            "test-project.capstone_ds.partner_stock_aws_replica")


def test_aws_restock_reports_failure(config):
    from storefront.ops import AwsRestock, PartnerListing

    class Client:
        def __init__(self, location):
            pass

        def query(self, sql, job_config):
            raise RuntimeError("Access Denied")

    job = AwsRestock(_aws_config(config), [PartnerListing("LST-0017", "MKT-S004", "BEA-3001-OM", "BEA-3001")],
                     {"LST-0017": 600}, client_factory=Client).run()
    assert job.state == "FAILED" and "Access Denied" in job.error
