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

import pytest

from storefront import data_agent as da

NAME = "projects/test-project/locations/global/dataAgents/yeyo-viral-analyst"


class FakeResp:
    def __init__(self, status=200, body=None):
        self.status_code, self._body = status, body if body is not None else {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class FakeSession:
    def __init__(self, get_status=404, chat=None, policy=None):
        self.calls, self.get_status, self.chat, self.policy = [], get_status, chat, policy or {}

    def get(self, url, **kw):
        self.calls.append(("GET", url, kw))
        return FakeResp(self.get_status)

    def patch(self, url, **kw):
        self.calls.append(("PATCH", url, kw))
        return FakeResp()

    def post(self, url, **kw):
        self.calls.append(("POST", url, kw))
        if url.endswith(":chat"):
            return FakeResp(body=self.chat)
        if url.endswith(":getIamPolicy"):
            return FakeResp(body=self.policy)
        return FakeResp()


CHAT_REPLY = [
    {"systemMessage": {"text": {"parts": ["Retrieving context for 12 tables."], "textType": "PROGRESS"}}},
    {"systemMessage": {"data": {"generatedSql": "SELECT 1 AS n"}}},
    {"systemMessage": {"data": {"bigQueryJob": {"projectId": "test-project", "jobId": "job_1",
                                                 "location": "us-central1"}}}},
    {"systemMessage": {"data": {"result": {"data": [{"n": 1}]}}}},
    {"systemMessage": {"chart": {"query": {"instructions": "line chart"}}}},
    {"systemMessage": {"chart": {"result": {"vegaConfig": {"mark": "line"}}}}},
    {"systemMessage": {"text": {"parts": ["Peak was ", "**1**."], "textType": "FINAL_RESPONSE"}}},
    {"systemMessage": {"text": {"parts": ["Why?", "What next?"], "textType": "FOLLOWUP_QUESTIONS"}}},
]


def test_agent_covers_the_viral_tables_with_examples(config):
    ctx = da.agent_body(config)["dataAnalyticsAgent"]["publishedContext"]
    tables = [t["tableId"] for t in ctx["datasourceReferences"]["bq"]["tableReferences"]]
    assert {"clickstream_events", "online_orders", "marketplace_orders", "vw_oversell_risk", "vw_partner_stock",
            "stock_master_location", "location_master", "customer_master"} <= set(tables)
    assert all(t["projectId"] == "test-project" and t["datasetId"] == "capstone_ds"
               for t in ctx["datasourceReferences"]["bq"]["tableReferences"])
    assert len(ctx["exampleQueries"]) == len(da.EXAMPLE_QUERIES)
    assert all("`test-project.capstone_ds." in q["sqlQuery"] for q in ctx["exampleQueries"])
    assert "YeYo" in ctx["systemInstruction"]


@pytest.mark.parametrize("exists,method,suffix", [(False, "POST", "dataAgents:createSync"),
                                                  (True, "PATCH", "yeyo-viral-analyst:updateSync")])
def test_ensure_agent_creates_or_updates(config, exists, method, suffix):
    s = FakeSession(get_status=200 if exists else 404)
    assert da.ensure_agent(config, session=s) == NAME
    verb, url, kw = s.calls[-1]
    assert verb == method and url.endswith(suffix)
    assert kw["json"]["displayName"] == "YeYo viral analyst"


def test_grant_users_adds_to_existing_members(config):
    s = FakeSession(policy={"etag": "e", "bindings": [{"role": da.AGENT_USER_ROLE, "members": ["user:a@x.com"]}]})
    da.grant_users(config, ["serviceAccount:sa@x.com"], session=s)
    verb, url, kw = s.calls[-1]
    assert url.endswith(":setIamPolicy")
    assert kw["json"]["policy"]["etag"] == "e"
    assert kw["json"]["policy"]["bindings"][0]["members"] == ["serviceAccount:sa@x.com", "user:a@x.com"]


def test_ask_sends_history_and_the_named_agent(config):
    s = FakeSession(chat=CHAT_REPLY)
    history = [{"userMessage": {"text": "earlier"}}, CHAT_REPLY[-2]]
    new = da.ask(config, history, "Which stores have stock?", session=s)
    _, url, kw = s.calls[-1]
    assert url.endswith("/projects/test-project/locations/global:chat")
    assert kw["json"]["messages"] == history + [{"userMessage": {"text": "Which stores have stock?"}}]
    assert kw["json"]["dataAgentContext"] == {"dataAgent": NAME, "contextVersion": "PUBLISHED"}
    assert new[0] == {"userMessage": {"text": "Which stores have stock?"}} and new[1:] == CHAT_REPLY


def test_parse_reply_collects_answer_sql_rows_chart_and_followups():
    r = da.parse_reply(CHAT_REPLY)
    assert r.text == "Peak was **1**."
    assert r.sql == "SELECT 1 AS n" and r.rows == [{"n": 1}] and r.chart == {"mark": "line"}
    assert r.followups == ["Why?", "What next?"] and r.error is None
    assert "j=bq:us-central1:job_1" in r.job_url


def test_parse_reply_uses_a_matched_example_query_and_reports_errors():
    r = da.parse_reply([
        {"systemMessage": {"data": {"matchedQuery": {"exampleQuery": {"sqlQuery": "SELECT 2"}}}}},
        {"systemMessage": {"error": {"text": "Table not found"}}},
    ])
    assert r.sql == "SELECT 2" and r.error == "Table not found" and r.text == ""


def test_parse_reply_keeps_only_the_last_query_rows():
    r = da.parse_reply([
        {"systemMessage": {"data": {"result": {"data": [{"n": 1}]}}}},
        {"systemMessage": {"data": {"generatedSql": "SELECT 0 LIMIT 0"}}},
        {"systemMessage": {"data": {"result": {"data": []}}}},
        {"systemMessage": {"data": {"result": None}}},
    ])
    assert r.sql == "SELECT 0 LIMIT 0" and r.rows == []
