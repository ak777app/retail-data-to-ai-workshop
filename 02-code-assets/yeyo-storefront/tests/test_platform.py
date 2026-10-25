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

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from storefront import governance
from storefront import platform as lake
from storefront.config import load_config
from storefront.ops import restock_batch


@pytest.fixture
def config(monkeypatch):
    monkeypatch.setenv("PROJECT_ID", "p1")
    return load_config()


def _aws(config):
    return config.__class__(**{**config.__dict__, "aws_s3_bucket": "yeyo-glowlab-1"})


class FakeResp:
    def __init__(self, body=None, status=200):
        self._body, self.status_code, self.ok = body or {}, status, status < 400

    def json(self):
        return self._body

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(self.status_code)


def test_partner_feed_is_jsonl_with_hero_units():
    rows = lake.partner_feed_rows(600, now=datetime(2026, 1, 1, tzinfo=timezone.utc))
    lines = lake.partner_feed_jsonl(rows).splitlines()
    assert [json.loads(line)["units_on_hand"] for line in lines] == [600, 1500, 1500]
    assert json.loads(lines[0]) == {"listing_id": "LST-0017", "seller_id": "MKT-S004", "omni_item_id": "BEA-3001-OM",
                                    "item_number": "BEA-3001", "warehouse_id": "GLOWLAB-EVANSTON-DC",
                                    "units_on_hand": 600, "snapshot_ts": "2026-01-01T00:00:00.000000Z",
                                    "source_system": "glowlab-spark"}


def test_upload_feed_and_spark_batch_reads_it(config):
    calls = []
    session = SimpleNamespace(post=lambda url, **kw: calls.append((url, kw)) or FakeResp())
    cfg = config.__class__(**{**config.__dict__, "lakehouse_bucket": "b1"})
    uri = lake.upload_partner_feed(cfg, lake.partner_feed_rows(600), session=session)
    assert uri.startswith("gs://b1/partner_feed/glowlab/stock-") and uri.endswith(".json")
    assert calls[0][1]["params"]["name"] == uri.removeprefix("gs://b1/")
    args = restock_batch(cfg, 600, feed_uri=uri)[1]["pysparkBatch"]["args"]
    assert args[-2:] == ["--feed", uri]
    assert "--feed" not in restock_batch(cfg, 600)[1]["pysparkBatch"]["args"]


def test_omni_query_runs_in_aws_region_and_reports_bytes(config):
    cfg = _aws(config)
    job = SimpleNamespace(result=lambda: [{"listing_id": "LST-0024", "units_on_hand": 40}], job_id="j1",
                          total_bytes_processed=746, total_bytes_billed=10 * 1024 * 1024)
    seen = {}
    client = SimpleNamespace(query=lambda sql, location: seen.update(sql=sql, location=location) or job)
    out = lake.run_omni_query(cfg, client=client)
    assert seen["location"] == "aws-us-east-1"
    assert "`p1.yeyo_partner_aws.fitgear_stock`" in seen["sql"]
    assert out["rows"] == [{"listing_id": "LST-0024", "units_on_hand": 40}]
    assert out["bytes_processed"] == 746 and out["result_bytes"] < 100
    assert lake.human_bytes(746) == "746 bytes" and lake.human_bytes(10 * 1024 * 1024) == "10.0 MB"


def test_lineage_walks_upstream_from_alerts(config):
    graph = {
        "bigquery:p1.capstone_ds.oversell_alerts": ["bigquery:p1.capstone_ds.vw_oversell_risk"],
        "bigquery:p1.capstone_ds.vw_oversell_risk": ["bigquery:p1.capstone_ds.vw_partner_stock",
                                                     "bigquery:p1.capstone_ds.clickstream_events"],
        "bigquery:p1.capstone_ds.vw_partner_stock": ["biglake:table:p1.yeyo_lake.yeyo_partner_lake.partner_stock"],
    }

    def post(url, json, timeout):
        assert url.endswith("/projects/p1/locations/us-central1:searchLinks")
        target = json["target"]["fullyQualifiedName"]
        return FakeResp({"links": [{"source": {"fullyQualifiedName": s}} for s in graph.get(target, [])]})

    edges = lake.lineage_edges(config, session=SimpleNamespace(post=post))
    assert len(edges) == 4
    dot = lake.lineage_dot(edges, lake.documented_edges(_aws(config)))
    assert '"partner_stock" -> "vw_partner_stock";' in dot
    assert '"fitgear_stock (S3, Omni)" -> "partner_stock_aws_replica" [style=dashed];' in dot
    assert '"Pub/Sub yeyo-clickstream" -> "clickstream_events" [style=dashed];' in dot


def test_governed_assets_cover_every_domain(config):
    assets = lake.governed_assets(_aws(config))
    assert {a.domain.split(" (")[0] for a in assets} == {"Clickstream", "Orders", "Partner stock", "Alerts",
                                                       "Customers"}
    fitgear = next(a for a in assets if a.cloud == "AWS")
    assert (fitgear.name, fitgear.catalog_location, fitgear.entry_group) == (
        "fitgear_stock", "us-east4", "@bigquery-aws-us-east-1")
    assert {"fitgear_stock", "partner_stock_aws_replica"}.isdisjoint(a.name for a in lake.governed_assets(config))
    assert lake.governed_search_url(config).endswith("&q=aspect%3Ayeyo-governed-asset")
    assert all(lake.asset_url(config, a).startswith("https://console.cloud.google.com/") for a in assets)


def test_search_governed_lists_entry_names(config):
    body = {"results": [{"dataplexEntry": {"name": "x/entries/bigquery.googleapis.com/projects/p1/datasets/d/tables/b"}},
                        {"dataplexEntry": {"name": "x/entries/bigquery.googleapis.com/projects/p1/datasets/d/tables/a"}}]}
    session = SimpleNamespace(post=lambda url, json, timeout: FakeResp(body))
    assert lake.search_governed(config, session=session) == ["a", "b"]


def test_governance_tags_every_asset_with_the_shared_aspect(config):
    patched = []

    def patch(url, params, json, timeout):
        patched.append((url, params, json))
        return FakeResp()

    assert governance.tag_assets(SimpleNamespace(patch=patch), _aws(config)) == len(lake.governed_assets(_aws(config)))
    url, params, body = patched[0]
    assert "/locations/us-central1/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/p1/" in url
    assert params["aspectKeys"] == "p1.global.yeyo-governed-asset"
    assert set(body["aspects"]["p1.global.yeyo-governed-asset"]["data"]) == set(governance.FIELDS)
    assert set(governance.PII_COLUMNS) == {"email", "phone_number"}


def test_grant_keeps_existing_members():
    sent = {}
    policy = {"etag": "e", "bindings": [{"role": "r1", "members": ["user:old"]}, {"role": "r2", "members": ["user:x"]}]}

    def post(url, json, timeout):
        if url.endswith(":getIamPolicy"):
            return FakeResp(policy)
        sent.update(json["policy"])
        return FakeResp()

    governance.grant(SimpleNamespace(post=post), "https://x/res", "r1", ["user:new"])
    assert {"role": "r1", "members": ["user:new", "user:old"]} in sent["bindings"]
    assert {"role": "r2", "members": ["user:x"]} in sent["bindings"] and sent["etag"] == "e"


def test_query_pii_refuses_untagged_columns(config):
    def field(name, tags):
        return SimpleNamespace(name=name, policy_tags=SimpleNamespace(names=tags) if tags else None)

    ran = []
    client = SimpleNamespace(query=lambda sql, job_config=None: ran.append(sql) or SimpleNamespace(result=lambda: []))
    client.get_table = lambda t: SimpleNamespace(schema=[field("email", ["t1"]), field("phone_number", None)])
    with pytest.raises(RuntimeError, match="phone_number"):
        lake.query_pii(config, client=client)
    assert not ran
    client.get_table = lambda t: SimpleNamespace(schema=[field("email", ["t1"]), field("phone_number", ["t2"])])
    assert lake.query_pii(config, client=client) == [] and ran
