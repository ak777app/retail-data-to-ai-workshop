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

"""Runtime configuration, read from environment variables / a local .env file.

Variable names match Module-11 (streaming) and Module-12 (storefront) lab manuals.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

APP_DIR = Path(__file__).resolve().parent.parent
SAMPLE_DATA_DIR = APP_DIR / "sample_data"

CHANNEL_IN_STORE = "in_store"
CHANNEL_ONLINE = "online"
CHANNEL_MARKETPLACE = "marketplace"
CHANNELS = (CHANNEL_IN_STORE, CHANNEL_ONLINE, CHANNEL_MARKETPLACE)

CLICKSTREAM_EVENT_TYPES = ("page_view", "add_to_cart", "waitlist_join", "purchase")


def is_clickstream_event(event: dict) -> bool:
    return event.get("event_type") in CLICKSTREAM_EVENT_TYPES


CATALOG_SOURCES = ("sample", "bigquery", "auto")
PUBLISH_MODES = ("auto", "pubsub", "bigquery", "local")


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class Config:
    project_id: str
    location: str
    bq_dataset: str
    pubsub_topic: str
    online_orders_table: str
    marketplace_orders_table: str
    instore_orders_table: str
    product_master_table: str
    marketplace_sellers_table: str
    marketplace_listings_table: str
    catalog_source: str
    publish_mode: str
    local_events_file: str
    spike_default_orders: int
    spike_max_orders: int
    spike_default_duration_seconds: int
    spike_max_duration_seconds: int
    clickstream_topic: str = "yeyo-clickstream"
    clickstream_table: str = "clickstream_events"
    oversell_view: str = "vw_oversell_risk"
    marketing_view: str = "vw_marketing_viral"
    oversell_alerts_table: str = "oversell_alerts"
    stock_location_table: str = "stock_master_location"
    partner_lake_dataset: str = "yeyo_partner_lake"
    partner_stock_table: str = "partner_stock"
    lakehouse_bucket: str = ""
    lakehouse_catalog: str = "yeyo_lake"
    spark_service_account: str = ""
    aws_connection: str = "aws-us-east-1.yeyo_aws_partner"
    aws_dataset: str = "yeyo_partner_aws"
    aws_s3_bucket: str = ""
    aws_s3_table: str = "fitgear_stock"
    partner_replica_table: str = "partner_stock_aws_replica"
    dq_scan_id: str = "yeyo-oversell-dq"
    agent_url: str = ""
    data_agent_url: str = ""
    data_agent_app: str = "data_analyst_agent"
    looker_report_url: str = ""
    spark_notebook_url: str = ""
    viral_listing_id: str = "LST-0017"
    viral_partner_units: int = 600
    viral_page_views: int = 13000
    viral_pause_units: int = 400
    viral_duration_seconds: int = 180
    viral_store_units: int = 400
    viral_pause_at_sell_out: bool = True

    @property
    def aws_location(self) -> str:
        return self.aws_connection.split(".", 1)[0]

    @property
    def topic_path(self) -> str:
        return f"projects/{self.project_id}/topics/{self.pubsub_topic}"

    @property
    def clickstream_topic_path(self) -> str:
        return f"projects/{self.project_id}/topics/{self.clickstream_topic}"

    def topic_path_for(self, event: dict) -> str:
        return self.clickstream_topic_path if is_clickstream_event(event) else self.topic_path

    def table_for_event(self, event: dict) -> str:
        if is_clickstream_event(event):
            return self.fq_table(self.clickstream_table)
        return self.orders_table_for_channel(event["channel"])

    def fq_table(self, table: str) -> str:
        return f"{self.project_id}.{self.bq_dataset}.{table}"

    def orders_table_for_channel(self, channel: str) -> str:
        tables = {
            CHANNEL_ONLINE: self.online_orders_table,
            CHANNEL_MARKETPLACE: self.marketplace_orders_table,
            CHANNEL_IN_STORE: self.instore_orders_table,
        }
        if channel not in tables:
            raise ValueError(f"Unknown channel: {channel!r}")
        return self.fq_table(tables[channel])


def load_config(env_file: Optional[os.PathLike] = None) -> Config:
    load_dotenv(env_file or APP_DIR / ".env", override=False)

    catalog_source = _env("CATALOG_SOURCE", "sample").lower()
    if catalog_source not in CATALOG_SOURCES:
        raise ValueError(f"CATALOG_SOURCE must be one of {CATALOG_SOURCES}, got {catalog_source!r}")
    publish_mode = _env("PUBLISH_MODE", "auto").lower()
    if publish_mode not in PUBLISH_MODES:
        raise ValueError(f"PUBLISH_MODE must be one of {PUBLISH_MODES}, got {publish_mode!r}")

    spike_max_orders = max(1, _env_int("SPIKE_MAX_ORDERS", 2000))
    spike_max_duration = max(0, _env_int("SPIKE_MAX_DURATION_SECONDS", 600))

    return Config(
        project_id=_env("PROJECT_ID", os.getenv("GOOGLE_CLOUD_PROJECT", "")),
        location=_env("LOCATION", "us-central1"),
        bq_dataset=_env("BQ_DATASET", "capstone_ds"),
        pubsub_topic=_env("PUBSUB_TOPIC", "yeyo-orders"),
        online_orders_table=_env("BQ_ONLINE_ORDERS_TABLE", "online_orders"),
        marketplace_orders_table=_env("BQ_MARKETPLACE_ORDERS_TABLE", "marketplace_orders"),
        instore_orders_table=_env("BQ_INSTORE_ORDERS_TABLE", "instore_orders"),
        product_master_table=_env("BQ_PRODUCT_MASTER_TABLE", "product_master"),
        marketplace_sellers_table=_env("BQ_MARKETPLACE_SELLERS_TABLE", "marketplace_seller_master"),
        marketplace_listings_table=_env("BQ_MARKETPLACE_LISTINGS_TABLE", "marketplace_listings"),
        catalog_source=catalog_source,
        publish_mode=publish_mode,
        local_events_file=_env("LOCAL_EVENTS_FILE", str(APP_DIR / "local_events.jsonl")),
        spike_default_orders=min(max(1, _env_int("SPIKE_DEFAULT_ORDERS", 200)), spike_max_orders),
        spike_max_orders=spike_max_orders,
        spike_default_duration_seconds=min(max(0, _env_int("SPIKE_DEFAULT_DURATION_SECONDS", 60)), spike_max_duration),
        spike_max_duration_seconds=spike_max_duration,
        clickstream_topic=_env("CLICKSTREAM_TOPIC", "yeyo-clickstream"),
        clickstream_table=_env("BQ_CLICKSTREAM_TABLE", "clickstream_events"),
        oversell_view=_env("BQ_OVERSELL_VIEW", "vw_oversell_risk"),
        marketing_view=_env("BQ_MARKETING_VIEW", "vw_marketing_viral"),
        oversell_alerts_table=_env("BQ_OVERSELL_ALERTS_TABLE", "oversell_alerts"),
        stock_location_table=_env("BQ_STOCK_LOCATION_TABLE", "stock_master_location"),
        partner_lake_dataset=_env("PARTNER_LAKE_DATASET", "yeyo_partner_lake"),
        partner_stock_table=_env("PARTNER_STOCK_TABLE", "partner_stock"),
        lakehouse_bucket=_env("LAKEHOUSE_BUCKET", ""),
        lakehouse_catalog=_env("LAKEHOUSE_CATALOG", "yeyo_lake"),
        spark_service_account=_env("SPARK_SERVICE_ACCOUNT", ""),
        aws_connection=_env("AWS_CONNECTION", "aws-us-east-1.yeyo_aws_partner"),
        aws_dataset=_env("AWS_DATASET", "yeyo_partner_aws"),
        aws_s3_bucket=_env("AWS_S3_BUCKET", ""),
        aws_s3_table=_env("AWS_S3_TABLE", "fitgear_stock"),
        partner_replica_table=_env("BQ_PARTNER_REPLICA_TABLE", "partner_stock_aws_replica"),
        dq_scan_id=_env("DQ_SCAN_ID", "yeyo-oversell-dq"),
        agent_url=_env("AGENT_URL", ""),
        data_agent_url=_env("DATA_AGENT_URL", "").rstrip("/"),
        data_agent_app=_env("DATA_AGENT_APP", "data_analyst_agent"),
        looker_report_url=_env("LOOKER_REPORT_URL", ""),
        spark_notebook_url=_env("SPARK_NOTEBOOK_URL", ""),
        viral_listing_id=_env("VIRAL_LISTING_ID", "LST-0017"),
        viral_partner_units=max(1, _env_int("VIRAL_PARTNER_UNITS", 600)),
        viral_page_views=max(1, _env_int("VIRAL_PAGE_VIEWS", 13000)),
        viral_pause_units=max(0, _env_int("VIRAL_PAUSE_UNITS", 400)),
        viral_duration_seconds=max(0, _env_int("VIRAL_DURATION_SECONDS", 180)),
        viral_store_units=max(0, _env_int("VIRAL_STORE_UNITS", 400)),
        viral_pause_at_sell_out=_env("VIRAL_PAUSE_AT_SELL_OUT", "true").lower() not in ("0", "false", "no"),
    )
