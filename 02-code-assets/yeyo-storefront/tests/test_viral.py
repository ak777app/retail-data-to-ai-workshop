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
from storefront.ops import demo_links, group_links, restock_batch, risk_level, submit_restock
from storefront.publisher import BigQueryPublisher, PubSubPublisher, event_attributes, event_to_row
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
    assert len(clicks) == snap["page_views"] + snap["add_to_cart"] + snap["waitlist"]
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
    assert all("project=test-project" in link.url for link in links)
    assert not any("storage/browser" in link.url for link in links)
    assert "3c · Engineering: Spark + Iceberg" in group_links(links)
    bucketed = demo_links(config.__class__(**{**config.__dict__, "lakehouse_bucket": "b1", "agent_url": "http://a"}))
    assert any("storage/browser/b1/iceberg" in link.url for link in bucketed)
    assert bucketed[-1].url == "http://a"


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
