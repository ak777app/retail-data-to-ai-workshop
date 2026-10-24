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

"""Client for the ADK data analyst agent (adk api_server / `adk deploy cloud_run`), used by the agent tab."""

import uuid
from typing import Dict, List, Optional, Tuple

import requests

LOCAL_HOSTS = ("http://localhost", "http://127.0.0.1")


def _headers(base_url: str) -> Dict[str, str]:
    """ID token for a private Cloud Run service; none for a local adk api_server."""
    if base_url.startswith(LOCAL_HOSTS):
        return {}
    import google.auth.transport.requests
    from google.oauth2 import id_token

    token = id_token.fetch_id_token(google.auth.transport.requests.Request(), base_url)
    return {"Authorization": f"Bearer {token}"}


def create_session(base_url: str, app_name: str, user_id: str, http=requests) -> str:
    resp = http.post(f"{base_url}/apps/{app_name}/users/{user_id}/sessions", json={},
                     headers=_headers(base_url), timeout=60)
    resp.raise_for_status()
    return resp.json()["id"]


def parse_events(events: List[Dict]) -> Tuple[str, List[Dict]]:
    """Final answer text plus the tool calls (name, args, result) the agent made on the way."""
    answer: List[str] = []
    steps: List[Dict] = []
    for event in events:
        for part in (event.get("content") or {}).get("parts") or []:
            if part.get("functionCall"):
                steps.append({"tool": part["functionCall"].get("name"), "args": part["functionCall"].get("args")})
            elif part.get("functionResponse"):
                resp = part["functionResponse"]
                match = next((s for s in reversed(steps) if s["tool"] == resp.get("name") and "result" not in s), None)
                if match is not None:
                    match["result"] = resp.get("response")
            elif part.get("text") and not part.get("thought"):
                answer.append(part["text"])
    return (answer[-1] if answer else ""), steps


def ask(base_url: str, app_name: str, user_id: str, session_id: str, question: str,
        http=requests) -> Tuple[str, List[Dict]]:
    body = {"app_name": app_name, "user_id": user_id, "session_id": session_id,
            "new_message": {"role": "user", "parts": [{"text": question}]}}
    resp = http.post(f"{base_url}/run", json=body, headers=_headers(base_url), timeout=600)
    resp.raise_for_status()
    return parse_events(resp.json())


def new_user_id(prefix: Optional[str] = "yeyo") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"
