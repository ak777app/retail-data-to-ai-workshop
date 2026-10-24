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
as `yeyo_partner_lake.partner_stock`. Each run appends one snapshot per listing.

  gcloud dataproc batches submit pyspark spark/partner_stock_sync.py -- --units 600
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession

# Glow Lab Beauty (MKT-S004) listings in sample_data/marketplace_listings.csv
LISTINGS = [
    ("LST-0017", "MKT-S004", "BEA-3001-OM", "BEA-3001"),
    ("LST-0018", "MKT-S004", "BEA-3002-OM", "BEA-3002"),
    ("LST-0019", "MKT-S004", "BEA-3003-OM", "BEA-3003"),
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="yeyo_lake")
    parser.add_argument("--namespace", default="yeyo_partner_lake")
    parser.add_argument("--table", default="partner_stock")
    parser.add_argument("--units", type=int, default=600, help="units on hand for the viral listing LST-0017")
    parser.add_argument("--other-units", type=int, default=1500)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("glowlab-partner-stock-sync").getOrCreate()
    fq = f"{args.catalog}.{args.namespace}.{args.table}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.namespace}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq} (
          listing_id STRING, seller_id STRING, omni_item_id STRING, item_number STRING,
          warehouse_id STRING, units_on_hand INT, snapshot_ts TIMESTAMP, source_system STRING
        ) USING iceberg
    """)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [(lid, sid, omni, item, "GLOWLAB-EVANSTON-DC", args.units if lid == "LST-0017" else args.other_units,
             now, "glowlab-spark") for lid, sid, omni, item in LISTINGS]
    df = spark.createDataFrame(rows, "listing_id string, seller_id string, omni_item_id string, item_number string, "
                                     "warehouse_id string, units_on_hand int, snapshot_ts timestamp, "
                                     "source_system string")
    df.writeTo(fq).append()
    spark.table(fq).orderBy("snapshot_ts", ascending=False).show(6, truncate=False)


if __name__ == "__main__":
    main()
