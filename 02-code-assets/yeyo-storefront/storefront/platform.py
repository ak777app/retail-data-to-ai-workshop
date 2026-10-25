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

"""Data platform tab: Spark JSON ingest, BigQuery Omni on S3, end-to-end lineage and one governed catalog."""

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, NamedTuple, Optional, Tuple
from urllib.parse import quote

from .config import Config
from .ops import CONSOLE, _authorized_session, _bq_table_url, _search_url

GOVERNED_ASPECT = "yeyo-governed-asset"
LINEAGE_API = "https://datalineage.googleapis.com/v1"
DATAPLEX_API = "https://dataplex.googleapis.com/v1"
GLOWLAB_LISTINGS = (("LST-0017", "BEA-3001"), ("LST-0018", "BEA-3002"), ("LST-0019", "BEA-3003"))


# ---------------------------------------------------------------- 1. Managed Spark: partner JSON -> Iceberg
def partner_feed_rows(units: int, other_units: int = 1500, now: Optional[datetime] = None) -> List[Dict]:
    """Glow Lab's stock feed as it arrives from the partner: one JSON record per listing."""
    ts = (now or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return [{"listing_id": lid, "seller_id": "MKT-S004", "omni_item_id": f"{item}-OM", "item_number": item,
             "warehouse_id": "GLOWLAB-EVANSTON-DC", "units_on_hand": units if lid == "LST-0017" else other_units,
             "snapshot_ts": ts, "source_system": "glowlab-spark"} for lid, item in GLOWLAB_LISTINGS]


def partner_feed_jsonl(rows: List[Dict]) -> str:
    return "".join(json.dumps(r) + "\n" for r in rows)


def upload_partner_feed(config: Config, rows: List[Dict], session=None) -> str:
    """Drops the partner's JSON feed in the lakehouse bucket, where the Spark batch picks it up."""
    name = f"partner_feed/glowlab/stock-{int(time.time())}-{uuid.uuid4().hex[:6]}.json"
    resp = _authorized_session(session).post(
        f"https://storage.googleapis.com/upload/storage/v1/b/{config.lakehouse_bucket}/o",
        params={"uploadType": "media", "name": name}, data=partner_feed_jsonl(rows).encode(),
        headers={"Content-Type": "application/json"}, timeout=30)
    resp.raise_for_status()
    return f"gs://{config.lakehouse_bucket}/{name}"


def iceberg_rows_sql(config: Config) -> str:
    return (f"SELECT listing_id, units_on_hand, snapshot_ts, source_system "
            f"FROM `{config.project_id}.{config.lakehouse_catalog}.{config.partner_lake_dataset}."
            f"{config.partner_stock_table}` ORDER BY snapshot_ts DESC, listing_id LIMIT 6")


# ---------------------------------------------------------------- 2. BigQuery Omni: query FitGear on AWS
def omni_sql(config: Config) -> str:
    """Low-stock question answered next to the data in S3; only the matching rows come back."""
    return (f"SELECT listing_id, item_number, units_on_hand, snapshot_ts\n"
            f"FROM `{config.project_id}.{config.aws_dataset}.{config.aws_s3_table}`\n"
            f"WHERE seller_id = 'MKT-S003' AND units_on_hand < 100")


def run_omni_query(config: Config, client=None) -> Dict:
    from google.cloud import bigquery

    client = client or bigquery.Client(project=config.project_id or None, location=config.aws_location)
    started = time.monotonic()
    job = client.query(omni_sql(config), location=config.aws_location)
    rows = [dict(r.items()) for r in job.result()]
    return {
        "rows": rows,
        "seconds": round(time.monotonic() - started, 1),
        "bytes_processed": job.total_bytes_processed or 0,
        "bytes_billed": job.total_bytes_billed or 0,
        "result_bytes": len(json.dumps(rows, default=str).encode()),
        "job_id": job.job_id,
        "location": config.aws_location,
    }


def human_bytes(n: int) -> str:
    for unit in ("bytes", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:,.0f} {unit}" if unit == "bytes" else f"{n:,.1f} {unit}"
        n /= 1024
    return str(n)


# ---------------------------------------------------------------- 3. End-to-end lineage
def short_name(fqn: str) -> str:
    return fqn.split(":", 1)[-1].split(".")[-1] if fqn.startswith(("bigquery:", "biglake:")) else fqn.split(":", 1)[-1]


def lineage_edges(config: Config, session=None, max_nodes: int = 30) -> List[Tuple[str, str]]:
    """Walks Dataplex lineage upstream from the alert table (live links recorded by BigQuery and Spark)."""
    s = _authorized_session(session)
    url = f"{LINEAGE_API}/projects/{config.project_id}/locations/{config.location}:searchLinks"
    start = f"bigquery:{config.fq_table(config.oversell_alerts_table)}"
    edges, seen, queue = [], set(), [start]
    while queue and len(seen) < max_nodes:
        target = queue.pop(0)
        if target in seen:
            continue
        seen.add(target)
        resp = s.post(url, json={"target": {"fullyQualifiedName": target}}, timeout=30)
        resp.raise_for_status()
        for link in resp.json().get("links", []):
            src = link["source"]["fullyQualifiedName"]
            if (src, target) not in edges and (target, src) not in edges:
                edges.append((src, target))
                queue.append(src)
    return edges


def documented_edges(config: Config) -> List[Tuple[str, str]]:
    """Hops lineage doesn't record (Pub/Sub subscriptions, the one-off cross-cloud load), drawn dashed."""
    edges = [(f"Pub/Sub {config.clickstream_topic}", config.clickstream_table),
             (f"Pub/Sub {config.pubsub_topic}", config.marketplace_orders_table),
             ("JSON feed (GCS)", config.partner_stock_table)]
    if config.aws_s3_bucket:
        edges.append((f"{config.aws_s3_table} (S3, Omni)", config.partner_replica_table))
    return edges


def lineage_dot(live: List[Tuple[str, str]], documented: List[Tuple[str, str]]) -> str:
    lines = ['digraph { rankdir=LR; node [shape=box, style="rounded,filled", fillcolor="#e8f0fe", fontsize=11];']
    for src, dst in live:
        if short_name(src) != short_name(dst):
            lines.append(f'"{short_name(src)}" -> "{short_name(dst)}";')
    for src, dst in documented:
        lines.append(f'"{src}" [fillcolor="#fef7e0"]; "{src}" -> "{dst}" [style=dashed];')
    lines.append("}")
    return "\n".join(lines)


# ---------------------------------------------------------------- 4. One governed catalog
class GovernedAsset(NamedTuple):
    name: str
    domain: str
    cloud: str
    storage: str
    sensitivity: str
    access_control: str
    owner: str
    catalog_location: str
    entry_group: str
    entry: str


def governed_assets(config: Config, aws_catalog_location: str = "us-east4") -> List[GovernedAsset]:
    p, ds, loc = config.project_id, config.bq_dataset, config.location
    bq = f"bigquery.googleapis.com/projects/{p}/datasets"
    dataset_iam = f"IAM on dataset {ds}"

    def table(name, domain, sensitivity, access, owner, storage="BigQuery"):
        return GovernedAsset(name, domain, "Google Cloud", storage, sensitivity, access, owner, loc, "@bigquery",
                             f"{bq}/{ds}/tables/{name}")

    assets = [
        table(config.clickstream_table, "Clickstream", "Confidential", dataset_iam, "Digital"),
        table(config.marketplace_orders_table, "Orders", "Confidential", dataset_iam, "Marketplace"),
        table(config.online_orders_table, "Orders", "Confidential", dataset_iam, "eCommerce"),
        GovernedAsset(config.partner_stock_table, "Partner stock (Glow Lab)", "Google Cloud", "Cloud Storage (Iceberg)",
                      "Internal", "IAM on BigLake catalog", "Marketplace", loc, "@biglake",
                      f"biglake.googleapis.com/projects/{p}/catalogs/{config.lakehouse_catalog}/namespaces/"
                      f"{config.partner_lake_dataset}/tables/{config.partner_stock_table}"),
        GovernedAsset(config.aws_s3_table, "Partner stock (FitGear)", "AWS", "Amazon S3 (Iceberg)", "Internal",
                      "IAM on connection + dataset", "Marketplace", aws_catalog_location,
                      f"@bigquery-{config.aws_location}", f"{bq}/{config.aws_dataset}/tables/{config.aws_s3_table}"),
        table(config.partner_replica_table, "Partner stock (FitGear)", "Internal", dataset_iam, "Marketplace"),
        table("vw_partner_stock", "Partner stock", "Internal", dataset_iam, "Marketplace"),
        table(config.oversell_view, "Alerts", "Internal", dataset_iam, "Operations"),
        table(config.oversell_alerts_table, "Alerts", "Internal", dataset_iam, "Operations"),
        table("customer_master", "Customers", "Restricted (PII)",
              "Policy tags: email and phone masked unless Fine-Grained Reader", "CRM"),
    ]
    return assets if config.aws_s3_bucket else [a for a in assets if a.domain != "Partner stock (FitGear)"]


def governed_search_url(config: Config) -> str:
    return _search_url(config.project_id, f"aspect:{GOVERNED_ASPECT}")


def asset_url(config: Config, asset: GovernedAsset) -> str:
    if asset.entry_group == "@biglake":
        return _search_url(config.project_id, asset.name)
    dataset = asset.entry.split("/datasets/", 1)[1].split("/", 1)[0]
    return _bq_table_url(config.project_id, dataset, asset.name)


def search_governed(config: Config, session=None) -> List[str]:
    resp = _authorized_session(session).post(
        f"{DATAPLEX_API}/projects/{config.project_id}/locations/global:searchEntries",
        json={"query": f"aspect:{GOVERNED_ASPECT}", "pageSize": 50}, timeout=30)
    resp.raise_for_status()
    return sorted(r["dataplexEntry"]["name"].rsplit("/", 1)[-1] for r in resp.json().get("results", []))


def pii_sql(config: Config) -> str:
    return (f"SELECT customer_id, first_name, email, phone_number FROM `{config.fq_table('customer_master')}` "
            "ORDER BY customer_id LIMIT 5")


def lineage_console_url(config: Config) -> str:
    return _bq_table_url(config.project_id, config.bq_dataset, config.oversell_alerts_table)


def policy_tags_url(config: Config) -> str:
    return f"{CONSOLE}/bigquery/policy-tags?project={quote(config.project_id)}"
