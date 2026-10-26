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

"""Marketplace partner (Glow Lab Beauty) stock sync, written by Spark as Apache Iceberg.

Runs on Google Cloud Serverless for Apache Spark with the BigLake metastore
Iceberg catalog, so the table is readable from BigQuery (and any Iceberg engine)
as `yeyo_partner_lake.partner_stock`. Each run appends one snapshot per listing,
read from the partner's JSON feed (--feed, newline-delimited JSON) or generated
from --units when no feed is given.

The block between the "shared with" markers is also the ingestion cell of
partner_stock_sync.ipynb, the notebook Demo links opens; a test
keeps the two identical.

  gcloud dataproc batches submit pyspark spark/partner_stock_sync.py -- --units 600
"""

import argparse  # noqa: I001 (the shared block below starts with its own imports)

from pyspark.sql import SparkSession

# --- shared with partner_stock_sync.ipynb ---
from datetime import datetime, timezone

LISTINGS = [
    ("LST-0017", "MKT-S004", "BEA-3001-OM", "BEA-3001"),
    ("LST-0018", "MKT-S004", "BEA-3002-OM", "BEA-3002"),
    ("LST-0019", "MKT-S004", "BEA-3003-OM", "BEA-3003"),
]
SCHEMA = ("listing_id string, seller_id string, omni_item_id string, item_number string, "
          "warehouse_id string, units_on_hand int, snapshot_ts timestamp, source_system string")


def ingest_partner_stock(spark, table, feed=None, units=600, other_units=1500):
    """Appends one stock snapshot per listing to the Iceberg table, read from the partner's JSON feed if given."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
          listing_id STRING, seller_id STRING, omni_item_id STRING, item_number STRING,
          warehouse_id STRING, units_on_hand INT, snapshot_ts TIMESTAMP, source_system STRING
        ) USING iceberg
    """)
    if feed:
        df = spark.read.schema(SCHEMA).json(feed)
    else:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows = [(lid, sid, omni, item, "GLOWLAB-EVANSTON-DC", units if lid == "LST-0017" else other_units,
                 now, "glowlab-spark") for lid, sid, omni, item in LISTINGS]
        df = spark.createDataFrame(rows, SCHEMA)
    df.writeTo(table).append()
    return spark.table(table).orderBy("snapshot_ts", ascending=False)
# --- end shared ---


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="yeyo_lake")
    parser.add_argument("--namespace", default="yeyo_partner_lake")
    parser.add_argument("--table", default="partner_stock")
    parser.add_argument("--units", type=int, default=600, help="units on hand for the viral listing LST-0017")
    parser.add_argument("--other-units", type=int, default=1500)
    parser.add_argument("--feed", help="gs:// URI of the partner's JSON stock feed")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("glowlab-partner-stock-sync").getOrCreate()
    latest = ingest_partner_stock(spark, f"{args.catalog}.{args.namespace}.{args.table}", feed=args.feed,
                                  units=args.units, other_units=args.other_units)
    latest.show(6, truncate=False)


if __name__ == "__main__":
    main()
