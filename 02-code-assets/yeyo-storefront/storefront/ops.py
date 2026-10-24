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

"""Ops tab helpers: BigQuery queries for the viral moment and console deep links."""

from typing import Dict, List, NamedTuple
from urllib.parse import quote

from .config import Config

CONSOLE = "https://console.cloud.google.com"


class DemoLink(NamedTuple):
    scene: str
    label: str
    url: str


def _bq_table_url(project: str, dataset: str, table: str) -> str:
    return f"{CONSOLE}/bigquery?project={project}&ws=!1m5!1m4!4m3!1s{project}!2s{dataset}!3s{table}"


def _search_url(project: str, query: str) -> str:
    return f"{CONSOLE}/dataplex/search?project={project}&q={quote(query)}"


def demo_links(config: Config) -> List[DemoLink]:
    p, ds = config.project_id, config.bq_dataset
    links = [
        DemoLink("2 · Goes viral", f"Pub/Sub topic {config.clickstream_topic}",
                 f"{CONSOLE}/cloudpubsub/topic/detail/{config.clickstream_topic}?project={p}"),
        DemoLink("2 · Goes viral", f"Pub/Sub topic {config.pubsub_topic}",
                 f"{CONSOLE}/cloudpubsub/topic/detail/{config.pubsub_topic}?project={p}"),
        DemoLink("2 · Goes viral", f"BigQuery {config.clickstream_table}",
                 _bq_table_url(p, ds, config.clickstream_table)),
        DemoLink("2 · Goes viral", f"BigQuery {config.marketplace_orders_table}",
                 _bq_table_url(p, ds, config.marketplace_orders_table)),
        DemoLink("3a · Oversell alert", f"BigQuery {config.oversell_view}", _bq_table_url(p, ds, config.oversell_view)),
        DemoLink("3a · Oversell alert", f"BigQuery {config.oversell_alerts_table}",
                 _bq_table_url(p, ds, config.oversell_alerts_table)),
        DemoLink("3a · Oversell alert", f"Data quality scan {config.dq_scan_id}",
                 f"{CONSOLE}/dataplex/govern/quality?project={p}"),
        DemoLink("3b · CDO: lineage & catalog", f"Lineage: {config.oversell_view} (open the Lineage tab)",
                 _bq_table_url(p, ds, config.oversell_view)),
        DemoLink("3b · CDO: lineage & catalog", "Knowledge Catalog: BEA-3001 assets",
                 _search_url(p, "BEA-3001 OR omni_item_id")),
        DemoLink("3c · Engineering: Spark + Iceberg", f"Knowledge Catalog: {config.partner_stock_table} (Iceberg)",
                 _search_url(p, config.partner_stock_table)),
        DemoLink("3c · Engineering: Spark + Iceberg", "Spark batch jobs (Serverless for Apache Spark)",
                 f"{CONSOLE}/dataproc/batches?project={p}"),
        DemoLink("3c · Engineering: Spark + Iceberg", "Lakehouse runtime catalog",
                 f"{CONSOLE}/bigquery/lakehouse?project={p}"),
    ]
    if config.lakehouse_bucket:
        links.append(DemoLink("3c · Engineering: Spark + Iceberg", "Cloud Storage: Parquet + Iceberg metadata",
                              f"{CONSOLE}/storage/browser/{config.lakehouse_bucket}/iceberg?project={p}"))
    if config.agent_url:
        links.append(DemoLink("4 · Agent acts", "Data analyst agent (adk web)", config.agent_url))
    return links


def group_links(links: List[DemoLink]) -> Dict[str, List[DemoLink]]:
    grouped: Dict[str, List[DemoLink]] = {}
    for link in links:
        grouped.setdefault(link.scene, []).append(link)
    return grouped


def oversell_sql(config: Config) -> str:
    return f"SELECT * FROM `{config.fq_table(config.oversell_view)}` WHERE listing_id = @listing_id"


def record_alert_sql(config: Config) -> str:
    return (
        f"INSERT INTO `{config.fq_table(config.oversell_alerts_table)}` "
        "(alert_ts, viral_id, listing_id, seller_id, omni_item_id, risk_level, listing_stock_status, "
        "partner_units, projected_units, cart_units_10m) "
        "SELECT CURRENT_TIMESTAMP(), @viral_id, listing_id, seller_id, omni_item_id, risk_level, "
        "listing_stock_status, partner_units, projected_units, cart_units_10m "
        f"FROM `{config.fq_table(config.oversell_view)}` WHERE listing_id = @listing_id AND risk_level != 'OK'"
    )


def per_minute_sql(config: Config) -> str:
    return f"""
SELECT TIMESTAMP_TRUNC(publish_time, MINUTE) AS minute,
       COUNTIF(event_type = 'page_view') AS page_views,
       COUNTIF(event_type = 'add_to_cart') AS add_to_cart,
       COUNTIF(event_type = 'waitlist_join') AS waitlist
FROM `{config.fq_table(config.clickstream_table)}`
WHERE publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
  AND marketplace_listing_id = @listing_id
GROUP BY minute ORDER BY minute"""


def store_stock_sql(config: Config) -> str:
    return f"""
SELECT location_id, quantity_on_hand
FROM `{config.fq_table(config.stock_location_table)}`
WHERE omni_item_id = @omni_item_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY stock_date DESC) = 1
ORDER BY quantity_on_hand DESC"""


def run_query(config: Config, sql: str, params: Dict[str, str], client=None) -> List[Dict]:
    from google.cloud import bigquery

    client = client or bigquery.Client(project=config.project_id or None, location=config.location)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()])
    return [dict(r.items()) for r in client.query(sql, job_config=job_config).result()]


def risk_level(projected_units: int, partner_units: int, at_risk_share: float = 0.25) -> str:
    if projected_units <= 0:
        return "OVERSOLD"
    if projected_units <= partner_units * at_risk_share:
        return "AT_RISK"
    return "OK"
