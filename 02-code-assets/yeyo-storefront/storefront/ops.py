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

import time
import uuid
from typing import Dict, List, NamedTuple, Optional, Tuple, Union
from urllib.parse import quote, urlencode

from .config import Config

CONSOLE = "https://console.cloud.google.com"
LOOKER_CREATE = "https://datastudio.google.com/reporting/create"
PARTNER_SOURCE_AWS = "aws"


class DemoLink(NamedTuple):
    scene: str
    label: str
    url: str


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


def demo_links(config: Config) -> List[DemoLink]:
    p, ds = config.project_id, config.bq_dataset
    looker_label = ("Looker Studio: oversell report" if config.looker_report_url
                    else "Looker Studio: new report on the oversell view")
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
                 f"{CONSOLE}/dataplex/govern/data-profiling-and-quality?project={p}"),
        DemoLink("3a · Oversell alert", looker_label, config.looker_report_url or looker_create_url(config)),
        DemoLink("3b · CDO: lineage & catalog", f"Lineage: {config.oversell_view} (open the Lineage tab)",
                 _bq_table_url(p, ds, config.oversell_view)),
        DemoLink("3b · CDO: lineage & catalog", "Knowledge Catalog: BEA-3001 assets",
                 _search_url(p, "BEA-3001 OR omni_item_id")),
    ]
    if config.partner_stock_source == PARTNER_SOURCE_AWS:
        scene, aws = "3c · Engineering: AWS S3 + BigQuery Omni", config.aws_dataset
        links += [
            DemoLink(scene, f"BigLake on S3 (Parquet): {aws}.{config.aws_s3_table}",
                     _bq_table_url(p, aws, config.aws_s3_table)),
            DemoLink(scene, f"BigLake on S3 (Iceberg): {aws}.{config.aws_iceberg_table}",
                     _bq_table_url(p, aws, config.aws_iceberg_table)),
            DemoLink(scene, f"Cross-cloud copy in GCP: {ds}.{config.partner_replica_table}",
                     _bq_table_url(p, ds, config.partner_replica_table)),
        ]
        if config.aws_s3_bucket:
            links.append(DemoLink(scene, f"Amazon S3: {config.aws_s3_bucket}",
                                  f"https://s3.console.aws.amazon.com/s3/buckets/{config.aws_s3_bucket}"
                                  f"?region={config.aws_location.removeprefix('aws-')}"))
        if config.agent_url:
            links.append(DemoLink("4 · Agent acts", "Data analyst agent (adk web)", config.agent_url))
        return links
    links += [
        DemoLink("3c · Engineering: Spark + Iceberg", f"Knowledge Catalog: {config.partner_stock_table} (Iceberg)",
                 _search_url(p, config.partner_stock_table)),
        DemoLink("3c · Engineering: Spark + Iceberg", "Spark batch jobs (Serverless for Apache Spark)",
                 f"{CONSOLE}/dataproc/batches?project={p}"),
        DemoLink("3c · Engineering: Spark + Iceberg", "Lakehouse runtime catalog",
                 f"{CONSOLE}/biglake?project={p}"),
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


def restock_batch(config: Config, units: int, batch_id: Optional[str] = None) -> Tuple[str, Dict]:
    """Serverless Spark batch that appends a fresh partner snapshot to the Iceberg feed (Module 13 section 3)."""
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
                     "--namespace", config.partner_lake_dataset, "--table", config.partner_stock_table],
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


def submit_restock(config: Config, units: int, session=None) -> str:
    batch_id, body = restock_batch(config, units)
    parent = f"projects/{config.project_id}/locations/{config.location}"
    resp = _authorized_session(session).post(f"{DATAPROC_API}/{parent}/batches", params={"batchId": batch_id},
                                             json=body, timeout=30)
    resp.raise_for_status()
    return f"{parent}/batches/{batch_id}"


def batch_state(name: str, session=None) -> str:
    resp = _authorized_session(session).get(f"{DATAPROC_API}/{name}", timeout=30)
    resp.raise_for_status()
    return resp.json().get("state", "PENDING")

PARTNER_COLUMNS = (
    "listing_id, seller_id, omni_item_id, item_number, warehouse_id, units_on_hand, snapshot_ts, source_system")


class PartnerListing(NamedTuple):
    listing_id: str
    seller_id: str
    omni_item_id: str
    item_number: str


def aws_restock_enabled(config: Config) -> bool:
    return config.partner_stock_source == PARTNER_SOURCE_AWS and bool(config.aws_s3_bucket)


def s3_export_sql(config: Config) -> str:
    """BigQuery Omni writes the partner snapshot into S3 as Parquet (runs in the AWS region, no AWS key)."""
    return f"""
EXPORT DATA WITH CONNECTION `{config.project_id}.{config.aws_connection}`
OPTIONS (uri = 's3://{config.aws_s3_bucket}/partner_stock/snapshot/*.parquet', format = 'PARQUET', overwrite = true) AS
SELECT r.listing_id, r.seller_id, r.omni_item_id, r.item_number, 'GLOWLAB-EVANSTON-DC' AS warehouse_id,
       r.units_on_hand, CURRENT_TIMESTAMP() AS snapshot_ts, 'glowlab-s3' AS source_system
FROM UNNEST(@rows) AS r"""


def omni_stage_sql(config: Config) -> str:
    """Cross-cloud CTAS: only the partner's filtered rows leave AWS, landing in the colocated region."""
    return f"""
CREATE OR REPLACE TABLE `{config.project_id}.{config.aws_stage_dataset}.{config.partner_stock_table}` AS
SELECT {PARTNER_COLUMNS}
FROM `{config.project_id}.{config.aws_dataset}.{config.aws_s3_table}`
WHERE seller_id = @seller_id"""


class AwsRestock:
    """Reset demo on AWS: S3 Parquet write, cross-cloud CTAS, then a copy next to the clickstream (~1 minute)."""

    def __init__(self, config: Config, listings: List[PartnerListing], units: Dict[str, int], client_factory=None):
        self.config, self.listings, self.units = config, listings, units
        self._client_factory = client_factory
        self.state, self.error = "PENDING", ""

    def _client(self, location: str):
        if self._client_factory:
            return self._client_factory(location)
        from google.cloud import bigquery
        return bigquery.Client(project=self.config.project_id or None, location=location)

    def run(self) -> "AwsRestock":
        from google.cloud import bigquery

        cfg = self.config
        rows = [bigquery.StructQueryParameter(
            None,
            bigquery.ScalarQueryParameter("listing_id", "STRING", x.listing_id),
            bigquery.ScalarQueryParameter("seller_id", "STRING", x.seller_id),
            bigquery.ScalarQueryParameter("omni_item_id", "STRING", x.omni_item_id),
            bigquery.ScalarQueryParameter("item_number", "STRING", x.item_number),
            bigquery.ScalarQueryParameter("units_on_hand", "INT64", int(self.units[x.listing_id])),
        ) for x in self.listings]
        try:
            self.state = "WRITING_S3"
            omni = self._client(cfg.aws_location)
            omni.query(s3_export_sql(cfg), job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("rows", "STRUCT", rows)])).result()
            self.state = "CROSS_CLOUD_CTAS"
            omni.query(omni_stage_sql(cfg), job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("seller_id", "STRING", self.listings[0].seller_id)])).result()
            self.state = "COPYING"
            self._client(cfg.location).copy_table(
                f"{cfg.project_id}.{cfg.aws_stage_dataset}.{cfg.partner_stock_table}",
                cfg.fq_table(cfg.partner_replica_table),
                job_config=bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")).result()
            self.state = "SUCCEEDED"
        except Exception as exc:
            self.state, self.error = "FAILED", f"{exc.__class__.__name__}: {str(exc)[:200]}"
        return self

    def start(self) -> "AwsRestock":
        import threading
        threading.Thread(target=self.run, daemon=True).start()
        return self
