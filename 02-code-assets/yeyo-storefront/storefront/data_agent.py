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

"""Talk to my Agent: a Gemini data agent (Conversational Analytics API) over the viral-moment tables.

  python -m storefront.data_agent YOUR_PROJECT_ID --user serviceAccount:yeyo-storefront-sa@...

creates or updates the agent and lets the storefront's service account chat with it. The agent queries
BigQuery as the caller, so customer email and phone stay masked for the storefront.
"""

import argparse
import sys
from dataclasses import replace
from typing import Dict, List, NamedTuple, Optional
from urllib.parse import quote

from .config import Config, load_config
from .ops import CONSOLE, _authorized_session

GDA_API = "https://geminidataanalytics.googleapis.com/v1beta"
AGENT_LOCATION = "global"
AGENT_USER_ROLE = "roles/geminidataanalytics.dataAgentUser"

SYSTEM_INSTRUCTION = """\
You are YeYo's retail data analyst. YeYo is a UK pharmacy, wellness and lifestyle retailer selling in stores,
online and on the YeYo Marketplace, where partner sellers such as Glow Lab Beauty list products.
- clickstream_events: shopper events from Pub/Sub (page_view, add_to_cart, waitlist_join, purchase); viral_id
  marks a viral spike.
- online_orders and marketplace_orders: orders from Pub/Sub; line_items is a repeated record;
  fulfillment_type CLICK_AND_COLLECT orders are collected from the store in location_id.
- vw_oversell_risk: partner stock from the seller's Iceberg feed minus orders since that snapshot; risk_level is
  OK, AT_RISK or OVERSOLD. vw_partner_stock is the partner feed itself.
- stock_master_location: store stock by location_id; location_master names the stores.
- oversell_alerts: alerts raised during a spike. customer_master: customers (email and phone are masked).
Answer briefly in British English, use product descriptions rather than IDs, and prefer the latest data."""

EXAMPLE_QUERIES = (
    ("How many shoppers viewed, added to cart and bought each product in the last viral spike?",
     "SELECT description, COUNTIF(event_type = 'page_view') AS page_views,\n"
     "  COUNTIF(event_type = 'add_to_cart') AS add_to_carts, COUNTIF(event_type = 'purchase') AS purchases\n"
     "FROM `{ds}.{clickstream}`\n"
     "WHERE viral_id = (SELECT viral_id FROM `{ds}.{clickstream}` WHERE viral_id IS NOT NULL\n"
     "                  ORDER BY event_ts DESC LIMIT 1)\n"
     "GROUP BY description ORDER BY page_views DESC"),
    ("Which marketplace listings are at risk of overselling right now?",
     "SELECT listing_id, omni_item_id, partner_units, units_ordered_since_snapshot, projected_units, risk_level\n"
     "FROM `{ds}.{oversell}` WHERE risk_level != 'OK' ORDER BY projected_units"),
)

SAMPLE_QUESTIONS = (
    "Chart add-to-cart events per minute during the last viral spike",
    "Which London stores still have Instant FirmX Temporary Eye Tightener in stock?",
    "How many click & collect orders did each store take today?",
)


class AgentReply(NamedTuple):
    text: str
    sql: Optional[str]
    rows: List[Dict]
    chart: Optional[Dict]
    followups: List[str]
    error: Optional[str]
    job_url: Optional[str] = None


def agent_name(config: Config) -> str:
    return f"projects/{config.project_id}/locations/{AGENT_LOCATION}/dataAgents/{config.data_agent_id}"


def agent_tables(config: Config) -> List[str]:
    return [config.clickstream_table, config.online_orders_table, config.marketplace_orders_table,
            config.product_master_table, config.marketplace_listings_table, config.marketplace_sellers_table,
            config.oversell_view, "vw_partner_stock", config.stock_location_table, "location_master",
            config.oversell_alerts_table, "customer_master"]


def agent_context(config: Config) -> Dict:
    ds = f"{config.project_id}.{config.bq_dataset}"
    fmt = {"ds": ds, "clickstream": config.clickstream_table, "oversell": config.oversell_view}
    return {
        "systemInstruction": SYSTEM_INSTRUCTION,
        "datasourceReferences": {"bq": {"tableReferences": [
            {"projectId": config.project_id, "datasetId": config.bq_dataset, "tableId": t}
            for t in agent_tables(config)]}},
        "exampleQueries": [{"naturalLanguageQuestion": q, "sqlQuery": sql.format(**fmt)}
                           for q, sql in EXAMPLE_QUERIES],
    }


def agent_body(config: Config) -> Dict:
    context = agent_context(config)
    return {"displayName": "YeYo viral analyst",
            "description": "Answers questions about YeYo's viral moment: clickstream, orders, partner and store stock.",
            "dataAnalyticsAgent": {"stagingContext": context, "publishedContext": context}}


def ensure_agent(config: Config, session=None) -> str:
    s = _authorized_session(session)
    name = agent_name(config)
    if s.get(f"{GDA_API}/{name}", timeout=30).status_code == 200:
        resp = s.patch(f"{GDA_API}/{name}:updateSync", timeout=120, json=agent_body(config),
                       params={"updateMask": "displayName,description,dataAnalyticsAgent"})
    else:
        resp = s.post(f"{GDA_API}/{name.rsplit('/dataAgents/', 1)[0]}/dataAgents:createSync", timeout=120,
                      params={"dataAgentId": config.data_agent_id}, json=agent_body(config))
    resp.raise_for_status()
    return name


def grant_users(config: Config, members: List[str], session=None) -> None:
    s = _authorized_session(session)
    url = f"{GDA_API}/{agent_name(config)}"
    resp = s.post(f"{url}:getIamPolicy", json={}, timeout=30)
    resp.raise_for_status()
    policy = resp.json()
    bindings = policy.setdefault("bindings", [])
    binding = next((b for b in bindings if b["role"] == AGENT_USER_ROLE), None)
    if binding is None:
        binding = {"role": AGENT_USER_ROLE, "members": []}
        bindings.append(binding)
    binding["members"] = sorted(set(binding["members"]) | set(members))
    s.post(f"{url}:setIamPolicy", json={"policy": policy}, timeout=30).raise_for_status()


def job_url(job: Dict) -> str:
    return (f"{CONSOLE}/bigquery?project={quote(job['projectId'])}"
            f"&j=bq:{job.get('location', 'US')}:{job['jobId']}&page=queryresults")


def parse_reply(messages: List[Dict]) -> AgentReply:
    texts, followups, rows, sql, chart, error, job = [], [], [], None, None, None, None
    for m in messages:
        sm = m.get("systemMessage") or {}
        if "text" in sm:
            kind, parts = sm["text"].get("textType"), sm["text"].get("parts", [])
            if kind == "FOLLOWUP_QUESTIONS":
                followups = list(parts)
            elif kind in (None, "FINAL_RESPONSE", "TEXT_TYPE_UNSPECIFIED"):
                texts.append("".join(parts))
        if "data" in sm:
            matched = (sm["data"].get("matchedQuery") or {}).get("exampleQuery") or {}
            sql = sm["data"].get("generatedSql") or matched.get("sqlQuery") or sql
            if sm["data"].get("bigQueryJob", {}).get("jobId"):
                job = job_url(sm["data"]["bigQueryJob"])
            if "result" in sm["data"]:
                rows = (sm["data"]["result"] or {}).get("data") or []
        if "chart" in sm and (sm["chart"].get("result") or {}).get("vegaConfig"):
            chart = sm["chart"]["result"]["vegaConfig"]
        if "error" in sm:
            error = sm["error"].get("text") or str(sm["error"])
    return AgentReply("\n\n".join(t for t in texts if t), sql, rows, chart, followups, error, job)


def ask(config: Config, history: List[Dict], question: str, session=None) -> List[Dict]:
    """Sends the conversation so far plus `question`; returns the new messages (user message first)."""
    user = {"userMessage": {"text": question}}
    resp = _authorized_session(session).post(
        f"{GDA_API}/projects/{config.project_id}/locations/{AGENT_LOCATION}:chat", timeout=300,
        json={"parent": f"projects/{config.project_id}/locations/{AGENT_LOCATION}",
              "messages": history + [user],
              "dataAgentContext": {"dataAgent": agent_name(config), "contextVersion": "PUBLISHED"}})
    resp.raise_for_status()
    body = resp.json()
    return [user] + (body if isinstance(body, list) else [body])


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("project_id")
    p.add_argument("--user", action="append", default=[],
                   help="IAM member allowed to chat, e.g. serviceAccount:yeyo-storefront-sa@PROJECT.iam.gserviceaccount.com")
    args = p.parse_args(argv)
    config = replace(load_config(), project_id=args.project_id)
    name = ensure_agent(config)
    if args.user:
        grant_users(config, args.user)
    print(f"Data agent ready: {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
