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

from storefront import agent


class FakeResponse:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class FakeHttp:
    def __init__(self, *bodies):
        self.bodies, self.calls = list(bodies), []

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append((url, json, headers))
        return FakeResponse(self.bodies.pop(0))


EVENTS = [
    {"content": {"role": "model", "parts": [{"functionCall": {"name": "sql_generator", "args": {"q": "stores"}}}]}},
    {"content": {"role": "user", "parts": [{"functionResponse": {"name": "sql_generator",
                                                                 "response": {"result": "SELECT 1"}}}]}},
    {"content": {"role": "model", "parts": [{"text": "thinking", "thought": True}, {"text": "Camden has 400."}]}},
]


def test_parse_events_returns_answer_and_tool_steps():
    answer, steps = agent.parse_events(EVENTS)
    assert answer == "Camden has 400."
    assert steps == [{"tool": "sql_generator", "args": {"q": "stores"}, "result": {"result": "SELECT 1"}}]


def test_local_agent_session_and_run_without_token():
    http = FakeHttp({"id": "s1"}, EVENTS)
    base = "http://localhost:8000"
    session = agent.create_session(base, "data_analyst_agent", "u1", http=http)
    answer, _ = agent.ask(base, "data_analyst_agent", "u1", session, "Where is stock?", http=http)
    assert session == "s1" and answer == "Camden has 400."
    assert http.calls[0][0] == f"{base}/apps/data_analyst_agent/users/u1/sessions" and http.calls[0][2] == {}
    url, body, _ = http.calls[1]
    assert url == f"{base}/run" and body["session_id"] == "s1"
    assert body["new_message"] == {"role": "user", "parts": [{"text": "Where is stock?"}]}
