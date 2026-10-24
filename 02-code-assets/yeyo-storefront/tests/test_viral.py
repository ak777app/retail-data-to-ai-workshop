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
from pathlib import Path

from storefront.ops import demo_links, group_links, risk_level
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


def test_risk_level():
    assert risk_level(600, 600) == "OK"
    assert risk_level(100, 600) == "AT_RISK"
    assert risk_level(0, 600) == "OVERSOLD"


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
