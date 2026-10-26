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

"""Demo center helpers: BigQuery queries for the viral moment and console deep links."""

import time
import uuid
from typing import Dict, List, NamedTuple, Optional, Tuple, Union
from urllib.parse import quote, urlencode

from .config import Config

CONSOLE = "https://console.cloud.google.com"
SPARK_NOTEBOOK = ("https://raw.githubusercontent.com/ak777app/retail-data-to-ai-workshop/main/"
                  "02-code-assets/yeyo-storefront/spark/partner_stock_sync.ipynb")
LOOKER_CREATE = "https://datastudio.google.com/reporting/create"
PARTNER_FEED_ASPECT = "yeyo-partner-feed"
AWS_S3_PREFIX = "iceberg/partner_stock/"


class DemoLink(NamedTuple):
    scene: str
    label: str
    url: str



def spark_notebook_url(config: Config) -> str:
    """Opens the partner-stock PySpark notebook (same code as the Reset batch job) in Colab Enterprise."""
    if config.spark_notebook_url.startswith(f"{CONSOLE}/"):
        return config.spark_notebook_url
    source = quote(config.spark_notebook_url or SPARK_NOTEBOOK, safe=":")
    return f"{CONSOLE}/vertex-ai/colab/import/{source}?project={config.project_id}"

def _bq_table_url(project: str, dataset: str, table: str) -> str:
    return f"{CONSOLE}/bigquery?project={project}&ws=!1m5!1m4!4m3!1s{project}!2s{dataset}!3s{table}"


def _search_url(project: str, query: str) -> str:
    return f"{CONSOLE}/dataplex/dp-search?project={project}&q={quote(query)}"


def looker_create_url(config: Config) -> str:
    """Linking API URL for a new Looker Studio report on the oversell view (default report template)."""
    params = {
        "r.reportName": "YeYo oversell alert",
        "ds.connector": "bigQuery",
        "ds.type": "TABLE",
        "ds.projectId": config.project_id,
        "ds.datasetId": config.bq_dataset,
        "ds.tableId": config.oversell_view,
        "ds.datasourceName": config.oversell_view,
    }
    return f"{LOOKER_CREATE}?{urlencode(params)}"


DEMO_SCENE = "Platform"
EXTRA_SCENE = "Additional links"


def demo_links(config: Config) -> List[DemoLink]:
    p = config.project_id
    links = [DemoLink(DEMO_SCENE, "Lineage", _bq_table_url(p, config.bq_dataset, config.oversell_view))]
    if config.aws_s3_bucket:
        links.append(DemoLink(DEMO_SCENE, "Multi-Cloud", _bq_table_url(p, config.aws_dataset, config.aws_s3_table)))
    links.append(DemoLink(DEMO_SCENE, "Unified Governance", _search_url(p, config.partner_stock_table)))
    links.append(DemoLink(EXTRA_SCENE, "Pyspark notebook", spark_notebook_url(config)))
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


def latest_clickstream_sql(config: Config, limit: int = 10) -> str:
    return f"""
SELECT publish_time, message_id, subscription_name, event_type, session_id, quantity, viral_id
FROM `{config.fq_table(config.clickstream_table)}`
WHERE publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND marketplace_listing_id = @listing_id
ORDER BY publish_time DESC LIMIT {int(limit)}"""


def store_stock_sql(config: Config) -> str:
    return f"""
SELECT location_id, quantity_on_hand
FROM `{config.fq_table(config.stock_location_table)}`
WHERE omni_item_id = @omni_item_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY stock_date DESC) = 1
ORDER BY quantity_on_hand DESC"""


def store_stock_reset_sql(config: Config) -> str:
    """Replaces today's stock rows for the hero SKU in the given stores (latest row per store wins)."""
    table = config.fq_table(config.stock_location_table)
    return f"""
DELETE FROM `{table}`
WHERE omni_item_id = @omni_item_id AND stock_date = CURRENT_DATE() AND location_id IN UNNEST(@locations);
INSERT INTO `{table}` (stock_date, item_number, omni_item_id, location_id, quantity_on_hand)
SELECT CURRENT_DATE(), @item_number, @omni_item_id, location_id, @units[OFFSET(i)]
FROM UNNEST(@locations) AS location_id WITH OFFSET i;"""


def reset_store_stock(config: Config, item_number: str, omni_item_id: str, locations: List[str],
                      units: Union[int, Dict[str, int]], client=None) -> None:
    """Writes the same units to every store, or per-store units from a {location_id: units} mapping."""
    from google.cloud import bigquery

    per_store = [int(units[loc] if isinstance(units, dict) else units) for loc in locations]

    client = client or bigquery.Client(project=config.project_id or None, location=config.location)
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("item_number", "STRING", item_number),
        bigquery.ScalarQueryParameter("omni_item_id", "STRING", omni_item_id),
        bigquery.ArrayQueryParameter("units", "INT64", per_store),
        bigquery.ArrayQueryParameter("locations", "STRING", list(locations)),
    ])
    client.query(store_stock_reset_sql(config), job_config=job_config).result()


def run_query(config: Config, sql: str, params: Dict[str, str], client=None) -> List[Dict]:
    from google.cloud import bigquery

    client = client or bigquery.Client(project=config.project_id or None, location=config.location)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()])
    return [dict(r.items()) for r in client.query(sql, job_config=job_config).result()]


def risk_level(projected_units: int, cart_units: int = 0) -> str:
    """Same rule as vw_oversell_risk: oversold at zero, at risk when open carts exceed what is left."""
    if projected_units <= 0:
        return "OVERSOLD"
    if projected_units < cart_units:
        return "AT_RISK"
    return "OK"


DATAPROC_API = "https://dataproc.googleapis.com/v1"
ICEBERG_JARS = ("iceberg-spark-runtime-3.5_2.13-1.10.2.jar", "iceberg-gcp-bundle-1.10.2.jar")
TERMINAL_BATCH_STATES = ("SUCCEEDED", "FAILED", "CANCELLED")


def restock_batch(config: Config, units: int, batch_id: Optional[str] = None,
                  feed_uri: Optional[str] = None) -> Tuple[str, Dict]:
    """Serverless Spark batch that ingests the partner's JSON feed into the Iceberg table (Module 13 section 3)."""
    bucket = f"gs://{config.lakehouse_bucket}"
    cat = f"spark.sql.catalog.{config.lakehouse_catalog}"
    properties = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        cat: "org.apache.iceberg.spark.SparkCatalog",
        f"{cat}.type": "rest",
        f"{cat}.uri": "https://biglake.googleapis.com/iceberg/v1/restcatalog",
        f"{cat}.warehouse": f"bl://projects/{config.project_id}/catalogs/{config.lakehouse_catalog}",
        f"{cat}.header.x-goog-user-project": config.project_id,
        f"{cat}.rest.auth.type": "org.apache.iceberg.gcp.auth.GoogleAuthManager",
        f"{cat}.io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
        "spark.dataproc.lineage.enabled": "true",
    }
    body: Dict = {
        "pysparkBatch": {
            "mainPythonFileUri": f"{bucket}/spark/partner_stock_sync.py",
            "jarFileUris": [f"{bucket}/jars/{jar}" for jar in ICEBERG_JARS],
            "args": ["--units", str(units), "--catalog", config.lakehouse_catalog,
                     "--namespace", config.partner_lake_dataset, "--table", config.partner_stock_table]
            + (["--feed", feed_uri] if feed_uri else []),
        },
        "runtimeConfig": {"version": "2.3", "properties": properties},
    }
    if config.spark_service_account:
        body["environmentConfig"] = {"executionConfig": {"serviceAccount": config.spark_service_account}}
    return batch_id or f"glowlab-restock-{int(time.time())}-{uuid.uuid4().hex[:6]}", body


def _authorized_session(session=None):
    if session is not None:
        return session
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(credentials)


def submit_restock(config: Config, units: int, session=None, feed_uri: Optional[str] = None) -> str:
    batch_id, body = restock_batch(config, units, feed_uri=feed_uri)
    parent = f"projects/{config.project_id}/locations/{config.location}"
    resp = _authorized_session(session).post(f"{DATAPROC_API}/{parent}/batches", params={"batchId": batch_id},
                                             json=body, timeout=30)
    resp.raise_for_status()
    return f"{parent}/batches/{batch_id}"


def batch_state(name: str, session=None) -> str:
    resp = _authorized_session(session).get(f"{DATAPROC_API}/{name}", timeout=30)
    resp.raise_for_status()
    return resp.json().get("state", "PENDING")
