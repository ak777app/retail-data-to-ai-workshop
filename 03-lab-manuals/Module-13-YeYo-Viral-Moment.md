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
| `storefront/marketing.py` | Marketing tab queries on `vw_marketing_viral` and the next-best-action rule |
| `sql/viral_moment.sql` | `clickstream_events`, `vw_oversell_risk`, `vw_marketing_viral`, `oversell_alerts` |

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

Each run appends a fresh snapshot (600 units for `LST-0017`). Before each demo, restock the partner either by re-running this command or by pressing **Reset demo** on the Ops tab (section 5). Check BigQuery reads the same table Spark wrote:

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
- **Demo links**: Pub/Sub topics, BigQuery tables, the data quality scan, a Looker Studio report, lineage, the Knowledge Catalog entry for the Iceberg table, Spark batches and the Cloud Storage folder.
- **Latest Pub/Sub events landed in BigQuery**: the newest `clickstream_events` rows with their Pub/Sub `message_id`, `publish_time` and `subscription_name`, to show the chart is backed by real data.

**Story flow (no clicks between beats except Go viral):**

1. Before the spike, the Ops tab shows marketing's current (simulated) bids on other products.
2. **Go viral**: shoppers, carts and orders climb; the spike pauses automatically at `VIRAL_PAUSE_UNITS` partner units left (default 400, "Demo settings"). A **Viral alert** pop-up shows shoppers in the last minute, carts and time to sell-out; its button deploys high-bid Google Shopping, Search and social campaigns on the hero product.
3. Open the **Marketing** tab (new bids, funnel, revenue at risk). Switching back to the Ops tab resumes the spike.
4. When partner stock runs out, a **Sold out** pop-up lists the London stores with stock; the hero campaigns pause and high-bid "collect in store today" local inventory ads plus ads for the private label **YeYo Instant Eye Firming Gel** (`BEA-3004`) start. The spike carries on as click & collect orders from store stock (`VIRAL_STORE_UNITS`, default 400 per store, set in `stock_master_location` by **Reset demo**).
5. When the stores sell out too, a **finish** pop-up compares units sold: Marketplace only (no lakehouse: the stores are never used) vs. Marketplace plus click & collect, about 3× with the defaults. On the **Shop** tab (online or marketplace channel) the product's **Add to cart** becomes **Click & collect**, which publishes an online order with `fulfillment_type = CLICK_AND_COLLECT` and the chosen store as `location_id`.

**Marketing: viral moment** tab (same run, read from `vw_marketing_viral`, refreshed every 5 seconds):

- KPIs: sessions, share from TikTok, conversion, revenue and revenue at risk (waitlist units × listing price);
- a headline action: scale spend while partner stock is OK, or pause ads to the listing, geo-target "collect in store today" and move budget to the own-brand substitute once it is at risk;
- funnel (page views, carts, purchases, waitlist) and sessions by traffic source;
- demand by postcode with the YeYo store in the same ZIP3 area that has stock (`location_master`, `stock_master_location`) and its next best action.

The simulator also sends a GA4-style `purchase` clickstream event with each order, so traffic source and postcode are attributed per session.

**Looker Studio (optional, alongside the Ops tab).** Demo links > **Looker Studio: new report on the oversell view** opens a new report (Linking API) already connected to `vw_oversell_risk`. Then:

1. Add a scorecard for `partner_units`, a scorecard for `projected_units` and a table with `listing_id`, `risk_level`, `units_ordered_since_snapshot` and `partner_snapshot_ts`. Give `risk_level` conditional formatting: red when it equals `OVERSOLD`.
2. **Add data** > BigQuery > `capstone_ds.clickstream_events`, and add a time series of record count by `publish_time` (minute) broken down by `event_type`.
3. **Resource** > **Manage added data sources** > edit each source > **Data freshness** > every 1 minute (the BigQuery default is 12 hours). Press **Refresh data** during the demo for an immediate update.
4. Save the report, then set `LOOKER_REPORT_URL` to its URL (`gcloud run services update yeyo-storefront --update-env-vars LOOKER_REPORT_URL=...`) so the button opens it.

The Ops tab polls BigQuery every 10 seconds; the report is at most a minute behind, so show the alert firing on the Ops tab and the report as the dashboard marketing already uses on the same view.

Lineage: open `vw_oversell_risk` > **Lineage**. It shows `clickstream_events`, `marketplace_orders` and the Spark-written Iceberg table `partner_stock` feeding the view. Lineage shows up a few minutes after the view is created, so build it well before the demo.

**Reset demo** clears the run and submits the same Spark batch from the storefront, with the units set in "Partner units on hand". It takes about 3 minutes, and **Go viral** is disabled until the batch finishes. To let the Cloud Run service account submit it:

```
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SA --role=roles/dataproc.editor --condition=None
gcloud iam service-accounts add-iam-policy-binding $SPARK_SA --member=serviceAccount:$SA --role=roles/iam.serviceAccountUser
```

Then run the scan (`gcloud dataplex datascans run yeyo-oversell-dq --location=$LOCATION`) to show the rule failing on the oversold listing.

## 6. Agent

The storefront's **Ask the data agent** tab calls the Module 21 data analyst agent through its ADK API server, so you don't need `adk web`. Deploy the agent as a private Cloud Run service with its own service account (it needs a `requirements.txt` inside the agent folder):

```
AGENT_SA=yeyo-data-agent-sa@$PROJECT_ID.iam.gserviceaccount.com
gcloud iam service-accounts create yeyo-data-agent-sa
for r in roles/bigquery.dataViewer roles/bigquery.jobUser roles/aiplatform.user roles/storage.objectViewer; do
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$AGENT_SA --role=$r --condition=None; done
mkdir -p ~/agent_stage && cp -r ~/retail-data-to-ai-workshop/02-code-assets/capstone-retail-solution/data_analyst_agent/data_analyst_agent ~/agent_stage/
cd ~/agent_stage && rm -f data_analyst_agent/.env data_analyst_agent/test.py
printf "python-dotenv\ngoogle-cloud-aiplatform\ngoogle-cloud-bigquery\ngoogle-cloud-storage\ngoogle-genai\nvertexai\npandas\ndb-dtypes\n" > data_analyst_agent/requirements.txt
adk deploy cloud_run --project $PROJECT_ID --region us-central1 --service_name yeyo-data-agent --app_name data_analyst_agent \
  --env GOOGLE_CLOUD_LOCATION=global --env GOOGLE_GENAI_USE_VERTEXAI=True --env GEMINI_MODEL=gemini-3.1-pro-preview \
  --env BQ_DATASET_IN_SCOPE=capstone_ds --env BQ_METADATA_BUCKET=capstone_stage_$PROJECT_NBR \
  --env BQ_METADATA_FILE=metadata/captone_database_metadata_grounding.md --env GOOGLE_CLOUD_PROJECT_NUMBER=$PROJECT_NBR \
  --provider-args=--service-account=$AGENT_SA --provider-args=--no-allow-unauthenticated ./data_analyst_agent
gcloud run services add-iam-policy-binding yeyo-data-agent --region us-central1 --member=serviceAccount:$SA --role=roles/run.invoker
gcloud run services update yeyo-storefront --region us-central1 \
  --update-env-vars DATA_AGENT_URL=$(gcloud run services describe yeyo-data-agent --region us-central1 --format='value(status.url)')
```

Locally, run `adk api_server` in `02-code-assets/capstone-retail-solution/data_analyst_agent` and set `DATA_AGENT_URL=http://localhost:8000`.

Ask: *"Where can we still fulfil BEA-3001-OM? List YeYo stores with stock on hand from stock_master_location for the latest stock_date."* The answer includes the SQL the agent ran; expand **Tool** to see the calls. Answers take about 30 seconds.

## 7. 5-minute talk track

| Time | Beat | On screen |
| :-- | :-- | :-- |
| 0:00 | Hook: one unpaid TikTok | Storefront: FirmX listing from Glow Lab, "In stock" |
| 0:20 | Goes viral | Go viral: live chart climbing (Pub/Sub ➜ BigQuery in seconds) |
| 1:10 | Oversell caught | Red alert + gauge; DQ scan link |
| 1:40 | CDO: can we trust it? | Lineage of `oversell_alerts`: clickstream + orders + Spark Iceberg feed |
| 2:10 | Head of Engineering: open formats | Spark batch ➜ Iceberg files in your bucket ➜ same table in BigQuery |
| 2:40 | Agent acts | Ask the data agent: "Where can we still fulfil…?" ➜ London stores; Marketing tab: pause ads, collect in store, private label ads |
| 3:10 | Strong finish | Stores sell out too: Marketplace units vs. Marketplace + click & collect, about 3× |
| 3:30 | Next-day contrast | Live vs. next-day chart; query in place day one, move only what must be real time |
| 4:30 | Close | "Same viral moment, same product. The only difference is whether your data arrives in minutes or tomorrow, and that's an architecture decision." |

## 8. Cleanup

```
gcloud dataplex datascans delete yeyo-oversell-dq --location=$LOCATION --quiet
gcloud pubsub subscriptions delete yeyo-clickstream-bq --quiet && gcloud pubsub topics delete yeyo-clickstream --quiet
gcloud biglake iceberg catalogs delete yeyo_lake --quiet
gcloud storage rm -r gs://$BUCKET
```

## 9. Optional: a second partner on Amazon S3 (BigQuery Omni)

Glow Lab's feed stays on Google Cloud, written by Spark into the Lakehouse Iceberg table. A second partner, FitGear Direct (`MKT-S003`, listings LST-0020/21/22/24), runs on AWS. BigQuery Omni writes and reads FitGear's stock in their S3 bucket, and only FitGear's filtered rows cross clouds. `vw_partner_stock` combines both feeds, so `vw_oversell_risk` covers both partners:

```
GCS  yeyo_lake.yeyo_partner_lake.partner_stock  (Iceberg, written by Spark: Glow Lab) ─────────────┐
S3   s3://yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID/partner_stock/  (Parquet, written by Omni)          │
  └─ BigLake table yeyo_partner_aws.partner_stock_s3 (aws-us-east-1, query runs in AWS)              │
       └─ cross-cloud CTAS ─► yeyo_partner_east4.partner_stock (us-east4)                            │
            └─ table copy ─► capstone_ds.partner_stock_aws_replica ─► vw_partner_stock (UNION ALL) ◄─┘
                                                                         └─► vw_oversell_risk
```

Omni needs a billed project (not the sandbox) and runs on demand in `aws-us-east-1`. A query in `us-central1` can't join the S3 table directly, and a cross-region join took about a minute, too slow for the Ops tab. So **Reset demo** runs the Spark batch for Glow Lab and, at the same time, the three Omni steps for FitGear (about 1 minute). The view then reads the local copy.

1. Create the connection and datasets, and note the identity the connection prints:

```
bq mk --connection --connection_type=AWS --location=aws-us-east-1 \
  --iam_role_id=arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/yeyo-bq-omni-partner-stock yeyo_aws_partner
bq mk --dataset --location=aws-us-east-1 yeyo_partner_aws
bq mk --dataset --location=us-east4 yeyo_partner_east4
```

2. In the AWS console (us-east-1):
   - **S3 bucket:** create `yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID` with Block all public access on and SSE-S3 encryption.
   - **IAM policy:** create `yeyo-bq-omni-partner-stock-s3`. It allows `s3:ListBucket` on the bucket, `s3:GetObject` on `bucket/*`, and `s3:PutObject` and `s3:DeleteObject` on `bucket/partner_stock/*`.
   - **IAM role:** create `yeyo-bq-omni-partner-stock` with the Web identity trusted entity type, Google as the provider, and the identity from step 1 as the audience. Attach the policy. Then edit the trust policy so its condition is `"accounts.google.com:sub": "IDENTITY"`.
   - **Session length:** set the role's maximum session duration to 12 hours.
3. Write the first snapshot and create the tables. **Reset demo** repeats the export, CTAS and copy:

```
bq query --use_legacy_sql=false --location=aws-us-east-1 "EXPORT DATA WITH CONNECTION \`$PROJECT_ID.aws-us-east-1.yeyo_aws_partner\` OPTIONS(uri='s3://yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID/partner_stock/snapshot/*.parquet', format='PARQUET', overwrite=true) AS SELECT 'LST-0020' AS listing_id, 'MKT-S003' AS seller_id, 'LIF-4001-OM' AS omni_item_id, 'LIF-4001' AS item_number, 'FITGEAR-INDIANAPOLIS-DC' AS warehouse_id, 1500 AS units_on_hand, CURRENT_TIMESTAMP() AS snapshot_ts, 'amazon-s3' AS source_system"
bq query --use_legacy_sql=false --location=aws-us-east-1 "CREATE OR REPLACE EXTERNAL TABLE \`$PROJECT_ID.yeyo_partner_aws.partner_stock_s3\` WITH CONNECTION \`$PROJECT_ID.aws-us-east-1.yeyo_aws_partner\` OPTIONS(format='PARQUET', uris=['s3://yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID/partner_stock/snapshot/*.parquet'])"
sed "s/PROJECT_ID/$PROJECT_ID/g" sql/partner_stock_aws.sql | bq query --use_legacy_sql=false --location=$LOCATION
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SA --role=roles/bigquery.connectionUser --condition=None
gcloud run services update yeyo-storefront --region us-central1 \
  --update-env-vars AWS_S3_BUCKET=yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID
```

Press **Reset demo** once before the first run so that the copy holds all four FitGear listings: 1,500 units each, and 40 for LOW_STOCK LST-0024. The Demo links panel adds a **3d · Engineering: AWS S3 + BigQuery Omni** scene next to the Spark links. To drop the AWS feed, unset `AWS_S3_BUCKET` and re-run `sql/viral_moment.sql`.

Optional, open format on the AWS side: Omni exports only CSV, JSON, Avro or Parquet, but it reads Iceberg. Have Athena (in the AWS console) create an Iceberg table of FitGear's stock, `fitgear.partner_stock_iceberg`, at `s3://yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID/iceberg/fitgear_partner_stock/`. Then point BigQuery at the table's latest metadata file:

```
bq query --use_legacy_sql=false --location=aws-us-east-1 "CREATE OR REPLACE EXTERNAL TABLE \`$PROJECT_ID.yeyo_partner_aws.partner_stock_iceberg\` WITH CONNECTION \`$PROJECT_ID.aws-us-east-1.yeyo_aws_partner\` OPTIONS(format='ICEBERG', uris=['s3://yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID/iceberg/fitgear_partner_stock/metadata/LATEST.metadata.json'])"
```

Replace `LATEST.metadata.json` with the newest `*.metadata.json` file name, and re-run the command after each Athena write.
