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

APP = str(Path(__file__).resolve().parent.parent / "app.py")


def test_app_places_order_and_runs_spike(monkeypatch, tmp_path):
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

    at.number_input(key="spike_orders").set_value(20).run()
    at.slider(key="spike_duration").set_value(0).run()
    next(b for b in at.button if b.label == "Simulate order spike").click().run()
    job = at.session_state["spike_job"]
    job.join(timeout=20)
    assert job.snapshot()["published"] == 20
    assert len((tmp_path / "events.jsonl").read_text().splitlines()) == 22


def test_ops_tab_runs_viral_moment_locally(monkeypatch, tmp_path):
    events_file = tmp_path / "events.jsonl"
    monkeypatch.setenv("PUBLISH_MODE", "local")
    monkeypatch.setenv("CATALOG_SOURCE", "sample")
    monkeypatch.setenv("LOCAL_EVENTS_FILE", str(events_file))
    monkeypatch.setenv("VIRAL_PAGE_VIEWS", "200")
    monkeypatch.setenv("VIRAL_DURATION_SECONDS", "0")
    monkeypatch.setenv("VIRAL_PARTNER_UNITS", "20")
    st.cache_resource.clear()
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert [t.label for t in at.tabs][2] == "Ops: viral moment"
    next(b for b in at.button if b.label == "Go viral").click().run()
    at.session_state["viral_job"].join(30)
    at.run()
    assert not at.exception
    assert any("OVERSELL" in e.value for e in at.error)
    lines = events_file.read_text().splitlines()
    assert sum('"page_view"' in line for line in lines) == 200
    assert [t.label for t in at.tabs][3] == "Marketing: viral moment"
    assert any(m.label == "Revenue at risk (waitlist)" for m in at.metric)
    assert any("Pause ads" in e.value for e in at.error)


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
