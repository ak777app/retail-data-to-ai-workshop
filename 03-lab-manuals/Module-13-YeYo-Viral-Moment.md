# Module 13: YeYo viral moment - clickstream, partner stock in Iceberg, oversell alert and agent

<hr>

## Motivation:

A niche cosmetic sold by marketplace partner Glow Lab Beauty (`MKT-S004`) goes viral on TikTok: **Instant FirmX Temporary Eye Tightener** (`BEA-3001-OM`, listing `LST-0017`). This module adds the 5-minute "viral moment" demo on top of Modules 11 and 12:

```
Storefront (Cloud Run) "Go viral"
  ├─ page views / add-to-carts / waitlist ─► Pub/Sub yeyo-clickstream ─► BQ capstone_ds.clickstream_events
  └─ marketplace orders ──────────────────► Pub/Sub yeyo-orders ───────► BQ capstone_ds.marketplace_orders
Glow Lab partner feed: Spark (Serverless for Apache Spark) ─► Apache Iceberg table in your bucket,
  registered in the Lakehouse runtime catalog (Iceberg REST) ─► BQ `PROJECT.yeyo_lake.yeyo_partner_lake.partner_stock`
                            │
  capstone_ds.vw_oversell_risk (partner units − orders since the snapshot) ─► Ops tab alert + capstone_ds.oversell_alerts
  Dataplex data quality scan yeyo-oversell-dq │ Data analyst agent: "Where can we still fulfil BEA-3001-OM?"
```

The listing keeps saying `IN_STOCK` while the partner's feed runs out; the view, the alert and the data quality rule catch it. Once the partner units are gone, late add-to-carts become waitlist joins.

| File | Purpose |
| :-- | :-- |
| `storefront/viral.py` | Viral simulator: rising traffic curve for one listing, clickstream + order events |
| `storefront/ops.py` | Ops tab queries, alert insert and the Demo links panel |
| `spark/partner_stock_sync.py` | Spark job that appends partner stock snapshots to the Iceberg table |
| `sql/viral_moment.sql` | `clickstream_events`, `vw_oversell_risk`, `oversell_alerts` |

<hr>

## Duration:

30 minutes setup (the Spark batch takes about 3 minutes), 5 minutes per demo run.

## Prerequisites:

Modules 11, 12 and 20 (capstone data, for store stock in `stock_master_location`). Module 21 for the agent.

<hr>

## 1. Variables

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID --format="value(projectNumber)"`
LOCATION="us-central1"
BUCKET=yeyo-lakehouse-$PROJECT_NBR
SPARK_SA=$PROJECT_NBR-compute@developer.gserviceaccount.com
cd ~/retail-data-to-ai-workshop/02-code-assets/yeyo-storefront
```

## 2. Bucket, Iceberg REST catalog and Spark permissions

```
gcloud services enable biglake.googleapis.com dataproc.googleapis.com dataplex.googleapis.com datalineage.googleapis.com
gcloud storage buckets create gs://$BUCKET --location=$LOCATION --uniform-bucket-level-access
gcloud biglake iceberg catalogs create yeyo_lake --project=$PROJECT_ID --catalog-type=biglake \
  --default-location=gs://$BUCKET/iceberg --credential-mode=end-user --primary-location=$LOCATION

# Serverless Spark needs Private Google Access on the subnet
gcloud compute networks subnets update default --region=$LOCATION --enable-private-ip-google-access

for R in roles/biglake.editor roles/dataproc.worker roles/datalineage.producer; do
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SPARK_SA --role=$R --condition=None
done
gcloud storage buckets add-iam-policy-binding gs://$BUCKET --member=serviceAccount:$SPARK_SA --role=roles/storage.objectUser
```

## 3. Spark writes the partner feed as Iceberg

The Iceberg Spark runtime and GCP bundle jars are staged in the bucket (Maven Central mirror from Google):

```
M=https://maven-central.storage-download.googleapis.com/maven2/org/apache/iceberg
for A in iceberg-spark-runtime-3.5_2.13 iceberg-gcp-bundle; do curl -sfO $M/$A/1.10.2/$A-1.10.2.jar; done
gcloud storage cp iceberg-*.jar gs://$BUCKET/jars/ && gcloud storage cp spark/partner_stock_sync.py gs://$BUCKET/spark/

C=spark.sql.catalog.yeyo_lake
gcloud dataproc batches submit pyspark gs://$BUCKET/spark/partner_stock_sync.py --region=$LOCATION --version=2.3 \
  --jars=gs://$BUCKET/jars/iceberg-spark-runtime-3.5_2.13-1.10.2.jar,gs://$BUCKET/jars/iceberg-gcp-bundle-1.10.2.jar \
  --properties="^#^spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions#$C=org.apache.iceberg.spark.SparkCatalog#$C.type=rest#$C.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog#$C.warehouse=bl://projects/$PROJECT_ID/catalogs/yeyo_lake#$C.header.x-goog-user-project=$PROJECT_ID#$C.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager#$C.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO#spark.dataproc.lineage.enabled=true" \
  -- --units 600
```

Each run appends a fresh snapshot (600 units for `LST-0017`). Re-run it before each demo to "restock" the partner. Check BigQuery reads the same table Spark wrote:

```
bq query --use_legacy_sql=false "SELECT * FROM \`$PROJECT_ID.yeyo_lake.yeyo_partner_lake.partner_stock\` ORDER BY snapshot_ts DESC"
gcloud storage ls -r gs://$BUCKET/iceberg/ | head
```

Note: BigQuery can't run DDL/DML on Iceberg REST catalog tables; Spark (or any Iceberg engine) owns writes.

## 4. Clickstream topic, tables, view and data quality scan

```
sed "s/PROJECT_ID/$PROJECT_ID/g" sql/viral_moment.sql | bq query --use_legacy_sql=false
gcloud pubsub topics create yeyo-clickstream
gcloud pubsub subscriptions create yeyo-clickstream-bq --topic=yeyo-clickstream \
  --bigquery-table=$PROJECT_ID:capstone_ds.clickstream_events --use-table-schema --write-metadata --drop-unknown-fields

cat > /tmp/dq.yaml <<'YAML'
rules:
- name: partner-stock-covers-demand
  dimension: VALIDITY
  rowConditionExpectation:
    sqlExpression: "NOT (listing_stock_status = 'IN_STOCK' AND projected_units <= 0)"
  threshold: 1.0
- name: partner-snapshot-fresh
  dimension: FRESHNESS
  rowConditionExpectation:
    sqlExpression: "partner_snapshot_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)"
  threshold: 1.0
YAML
gcloud dataplex datascans create data-quality yeyo-oversell-dq --location=$LOCATION \
  --data-source-resource=//bigquery.googleapis.com/projects/$PROJECT_ID/datasets/capstone_ds/tables/vw_oversell_risk \
  --data-quality-spec-file=/tmp/dq.yaml
```

The storefront service account also needs to read the Iceberg table:

```
SA=yeyo-storefront-sa@$PROJECT_ID.iam.gserviceaccount.com
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SA --role=roles/biglake.viewer --condition=None
gcloud storage buckets add-iam-policy-binding gs://$BUCKET --member=serviceAccount:$SA --role=roles/storage.objectViewer
```

## 5. Run the storefront with the Ops tab

Set `LAKEHOUSE_BUCKET=$BUCKET` (and optionally `AGENT_URL` to your `adk web` or Agent Engine URL) in `.env`, or on Cloud Run with `gcloud run services update yeyo-storefront --update-env-vars LAKEHOUSE_BUCKET=$BUCKET`. Open **Ops: viral moment** and press **Go viral**:

- live page views, add-to-carts, orders and waitlist, charted per 10 seconds;
- the partner-stock gauge (from `vw_oversell_risk`, refreshed every 10 seconds) and the red oversell alert, which is also written to `oversell_alerts`;
- a simulated marketing card: keywords, branches with stock for "collect in store today", and the substitute product once partner stock is gone;
- live vs. next-day batch report for the same orders;
- **Demo links**: Pub/Sub topics, BigQuery tables, the data quality scan, lineage, the Knowledge Catalog entry for the Iceberg table, Spark batches and the Cloud Storage folder.

Then run the scan (`gcloud dataplex datascans run yeyo-oversell-dq --location=$LOCATION`) to show the rule failing on the oversold listing.

## 6. Agent

Ask the data analyst agent (Module 21): *"Where can we still fulfil BEA-3001-OM? List YeYo stores with stock on hand from stock_master_location for the latest stock_date."*

## 7. 5-minute talk track

| Time | Beat | On screen |
| :-- | :-- | :-- |
| 0:00 | Hook: one unpaid TikTok | Storefront: FirmX listing from Glow Lab, "In stock" |
| 0:20 | Goes viral | Go viral: live chart climbing (Pub/Sub ➜ BigQuery in seconds) |
| 1:10 | Oversell caught | Red alert + gauge; DQ scan link |
| 1:40 | CDO: can we trust it? | Lineage of `oversell_alerts`: clickstream + orders + Spark Iceberg feed |
| 2:10 | Head of Engineering: open formats | Spark batch ➜ Iceberg files in your bucket ➜ same table in BigQuery |
| 2:40 | Agent acts | "Where can we still fulfil…?" ➜ branches; campaign card, waitlist |
| 3:30 | Next-day contrast | Live vs. next-day chart; query in place day one, move only what must be real time |
| 4:30 | Close | "Same viral moment, same product. The only difference is whether your data arrives in minutes or tomorrow, and that's an architecture decision." |

## 8. Cleanup

```
gcloud dataplex datascans delete yeyo-oversell-dq --location=$LOCATION --quiet
gcloud pubsub subscriptions delete yeyo-clickstream-bq --quiet && gcloud pubsub topics delete yeyo-clickstream --quiet
gcloud biglake iceberg catalogs delete yeyo_lake --quiet
gcloud storage rm -r gs://$BUCKET
```
