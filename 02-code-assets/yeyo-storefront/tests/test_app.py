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

from pathlib import Path

import streamlit as st
from streamlit.testing.v1 import AppTest

from storefront import data_agent

APP = str(Path(__file__).resolve().parent.parent / "app.py")


def test_app_places_orders(monkeypatch, tmp_path):
    monkeypatch.setenv("PUBLISH_MODE", "local")
    monkeypatch.setenv("CATALOG_SOURCE", "sample")
    monkeypatch.setenv("LOCAL_EVENTS_FILE", str(tmp_path / "events.jsonl"))
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert not at.exception

    at.button(key="add_PHA-1001-OM").click().run()
    assert len(at.session_state["cart"]) == 1
    next(b for b in at.button if b.label == "Place order").click().run()
    assert not at.exception
    assert at.session_state["orders"][0]["channel"] == "online"
    assert any("published" in s.value for s in at.success)

    at.radio(key="channel").set_value("marketplace").run()
    at.button(key="add_LIF-4001-OM").click().run()
    next(b for b in at.button if b.label == "Place order").click().run()
    order = at.session_state["orders"][0]
    assert order["channel"] == "marketplace" and order["line_items"][0]["marketplace_seller_id"] == "MKT-S003"



def test_ops_tab_runs_viral_moment_locally(monkeypatch, tmp_path):
    events_file = tmp_path / "events.jsonl"
    monkeypatch.setenv("PUBLISH_MODE", "local")
    monkeypatch.setenv("CATALOG_SOURCE", "sample")
    monkeypatch.setenv("LOCAL_EVENTS_FILE", str(events_file))
    monkeypatch.setenv("VIRAL_PAGE_VIEWS", "1000")
    monkeypatch.setenv("VIRAL_DURATION_SECONDS", "0")
    monkeypatch.setenv("VIRAL_PARTNER_UNITS", "20")
    monkeypatch.setenv("VIRAL_STORE_UNITS", "5")
    monkeypatch.setenv("VIRAL_PAUSE_AT_SELL_OUT", "false")
    st.cache_resource.clear()
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert [t.label for t in at.tabs] == ["Shop", "Demo center", "Marketing", "Engineering", "Governance",
                                          "Talk to my Agent"]
    next(b for b in at.button if b.label == "Go viral").click().run()
    at.session_state["viral_job"].join(30)
    at.run()
    assert not at.exception
    assert any("OVERSELL" in e.value for e in at.error)
    lines = events_file.read_text().splitlines()
    assert sum('"page_view"' in line for line in lines) == 1000
    snap = at.session_state["viral_job"].snapshot()
    assert snap["store_units_sold"] == snap["store_units_total"] == 15 and snap["store_units_left"] == 0
    assert any(m.label == "London store units left" for m in at.metric)
    assert any("click & collect" in md.value for md in at.markdown)


def test_ops_reset_demo_clears_run_locally(monkeypatch, tmp_path):
    monkeypatch.setenv("PUBLISH_MODE", "local")
    monkeypatch.setenv("CATALOG_SOURCE", "sample")
    monkeypatch.setenv("LOCAL_EVENTS_FILE", str(tmp_path / "events.jsonl"))
    monkeypatch.setenv("VIRAL_PAGE_VIEWS", "50")
    monkeypatch.setenv("VIRAL_DURATION_SECONDS", "0")
    st.cache_resource.clear()
    at = AppTest.from_file(APP, default_timeout=30).run()
    next(b for b in at.button if b.label == "Go viral").click().run()
    at.session_state["viral_job"].join(30)
    at.run()
    next(b for b in at.button if b.label == "Reset demo").click().run()
    assert not at.exception
    assert at.session_state["viral_job"] is None
    assert any("Demo reset" in i.value for i in at.info)


def test_viral_story_pauses_resumes_and_switches_shop_to_click_and_collect(monkeypatch, tmp_path):
    monkeypatch.setenv("PUBLISH_MODE", "local")
    monkeypatch.setenv("CATALOG_SOURCE", "sample")
    monkeypatch.setenv("LOCAL_EVENTS_FILE", str(tmp_path / "events.jsonl"))
    monkeypatch.setenv("VIRAL_PAGE_VIEWS", "400")
    monkeypatch.setenv("VIRAL_DURATION_SECONDS", "0")
    monkeypatch.setenv("VIRAL_PARTNER_UNITS", "40")
    monkeypatch.setenv("VIRAL_PAUSE_UNITS", "20")
    st.cache_resource.clear()
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert any(df.value.shape[0] >= 1 for df in at.dataframe)  # baseline bids
    next(b for b in at.button if b.label == "Go viral").click().run()
    job = at.session_state["viral_job"]
    for _ in range(100):
        if job.is_paused:
            break
        job.join(0.05)
    assert job.is_paused and job.projected_units <= 20
    at.run()
    assert at.session_state["open_dialog"] == "surge"

    at.session_state["open_dialog"] = None
    at.session_state["prev_nav"] = "Shop"
    at.session_state["nav"] = "Demo center"
    at.run()
    for _ in range(600):
        if job.is_paused:
            break
        job.join(0.05)
    assert job.is_paused and job.sell_out_pause_done and job.projected_units <= 0 and job.store_units_sold == 0
    at.run()
    assert not at.exception
    assert at.session_state["open_dialog"] == "sold_out"
    assert at.session_state["hero_sold_out"] == "BEA-3001-OM"
    at.button(key="collect_btn_BEA-3001-OM").click().run()
    order = at.session_state["orders"][0]
    assert order["fulfillment_type"] == "CLICK_AND_COLLECT" and order["location_id"].endswith("-ST")

    at.session_state["open_dialog"] = None
    at.session_state["prev_nav"] = "Shop"
    at.session_state["nav"] = "Demo center"
    at.run()
    job.join(30)
    assert not job.is_running and job.store_units_sold > 0


def test_every_product_has_a_bundled_image():
    import csv

    from storefront.catalog import product_from_row, product_image
    from storefront.config import SAMPLE_DATA_DIR

    with open(SAMPLE_DATA_DIR / "product_master.csv", newline="") as f:
        products = [product_from_row(row) for row in csv.DictReader(f)]
    assert products and not [p.item_number for p in products if not product_image(p)]


def test_engineering_and_governance_tabs_have_the_platform_buttons(monkeypatch, tmp_path):
    monkeypatch.setenv("PUBLISH_MODE", "local")
    monkeypatch.setenv("CATALOG_SOURCE", "sample")
    monkeypatch.setenv("LOCAL_EVENTS_FILE", str(tmp_path / "events.jsonl"))
    monkeypatch.setenv("AWS_S3_BUCKET", "yeyo-glowlab-1")
    st.cache_resource.clear()
    at = AppTest.from_file(str(APP), default_timeout=30).run()
    assert not at.exception
    assert [b.label for b in at.link_button] == ["Pyspark notebook", "Multi-Cloud", "Lineage", "Unified Governance"]
    assert at.button(key="plat_pii").label == "Data Protection" and at.button(key="plat_pii").disabled
    labels = [b.label for b in at.button]
    assert "Run on AWS with BigQuery Omni" not in labels and "Load lineage" not in labels



def test_talk_to_my_agent_is_off_locally(monkeypatch, tmp_path):
    monkeypatch.setenv("PUBLISH_MODE", "local")
    monkeypatch.setenv("CATALOG_SOURCE", "sample")
    monkeypatch.setenv("LOCAL_EVENTS_FILE", str(tmp_path / "events.jsonl"))
    at = AppTest.from_file(str(APP), default_timeout=30).run()
    assert not at.exception
    assert at.chat_input(key="agent_q").disabled
    assert at.button(key="agent_sample_0").label == data_agent.SAMPLE_QUESTIONS[0]
    assert at.button(key="agent_sample_0").disabled


def test_talk_to_my_agent_shows_answer_sql_table_and_followups(monkeypatch, tmp_path):
    monkeypatch.setenv("PUBLISH_MODE", "local")
    monkeypatch.setenv("CATALOG_SOURCE", "sample")
    monkeypatch.setenv("LOCAL_EVENTS_FILE", str(tmp_path / "events.jsonl"))
    at = AppTest.from_file(str(APP), default_timeout=30)
    reply = data_agent.AgentReply("Covent Garden has **400** left.", "SELECT 400", [{"store": "Covent Garden"}],
                                  None, ["And Camden?"], None, "https://console.cloud.google.com/bigquery?j=1")
    at.session_state["agent_turns"] = [{"q": "Which stores have FirmX?", "reply": reply},
                                       {"q": "Second?", "error": "Agent call failed: boom"}]
    at.run()
    assert not at.exception
    assert any("Covent Garden has **400** left." in m.value for m in at.markdown)
    assert [c.value for c in at.code] == ["SELECT 400"]
    assert any(list(d.value.columns) == ["store"] and d.value["store"].tolist() == ["Covent Garden"]
               for d in at.dataframe)
    assert "Open this job in BigQuery" in [b.label for b in at.link_button]
    assert at.error[0].value == "Agent call failed: boom"
    assert "agent_followup_0" not in [b.key for b in at.button]
