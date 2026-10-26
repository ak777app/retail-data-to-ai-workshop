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
  capstone_ds.vw_oversell_risk (partner units − orders since the snapshot) ─► Marketing tab alert + capstone_ds.oversell_alerts
  Dataplex data quality scan yeyo-oversell-dq │ Data analyst agent: "Where can we still fulfil BEA-3001-OM?"
```

The listing keeps saying `IN_STOCK` while the partner's feed runs out; the view, the alert and the data quality rule catch it. Once the partner units are gone, late add-to-carts become waitlist joins.

| File | Purpose |
| :-- | :-- |
| `storefront/viral.py` | Viral simulator: rising traffic curve for one listing, clickstream + order events |
| `storefront/ops.py` | Demo center queries, alert insert and the Engineering/Governance console links |
| `spark/partner_stock_sync.py` | Spark job that appends partner stock snapshots to the Iceberg table |
| `spark/partner_stock_sync.ipynb` | PySpark notebook with the same ingestion code, linked from the Engineering tab (**Pyspark notebook**) |
| `storefront/marketing.py` | Simulated campaign bids and banners for the Marketing tab |
| `sql/viral_moment.sql` | `clickstream_events`, `vw_oversell_risk`, `vw_marketing_viral`, `oversell_alerts` |
| `catalog/tag_partner_feeds.sh` | Tags every partner stock table, on Google Cloud and AWS, with one Knowledge Catalog aspect (section 9) |

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
for A in iceberg-spark-runtime-3.5_2.13 iceberg-spark-runtime-4.0_2.13 iceberg-gcp-bundle; do curl -sfO $M/$A/1.10.2/$A-1.10.2.jar; done
gcloud storage cp iceberg-*.jar gs://$BUCKET/jars/ && gcloud storage cp spark/partner_stock_sync.py gs://$BUCKET/spark/

C=spark.sql.catalog.yeyo_lake
gcloud dataproc batches submit pyspark gs://$BUCKET/spark/partner_stock_sync.py --region=$LOCATION --version=2.3 \
  --jars=gs://$BUCKET/jars/iceberg-spark-runtime-3.5_2.13-1.10.2.jar,gs://$BUCKET/jars/iceberg-gcp-bundle-1.10.2.jar \
  --properties="^#^spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions#$C=org.apache.iceberg.spark.SparkCatalog#$C.type=rest#$C.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog#$C.warehouse=bl://projects/$PROJECT_ID/catalogs/yeyo_lake#$C.header.x-goog-user-project=$PROJECT_ID#$C.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager#$C.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO#spark.dataproc.lineage.enabled=true" \
  -- --units 600
```

Without `--feed`, the job generates the snapshot from `--units`. The storefront's **Reset demo** (section 5) first drops Glow Lab's JSON stock feed in `gs://$BUCKET/partner_feed/glowlab/`, then pass it as `--feed gs://...json`, so Spark reads the partner's JSON and appends it to the Iceberg table. Each run appends a fresh snapshot (600 units for `LST-0017`). Before each demo, restock the partner either by re-running this command or by pressing **Reset demo** on the Demo center tab (section 5). Check BigQuery reads the same table Spark wrote:

```
bq query --use_legacy_sql=false "SELECT * FROM \`$PROJECT_ID.yeyo_lake.yeyo_partner_lake.partner_stock\` ORDER BY snapshot_ts DESC"
gcloud storage ls -r gs://$BUCKET/iceberg/ | head
```

Note: BigQuery can't run DDL/DML on Iceberg REST catalog tables; Spark (or any Iceberg engine) owns writes.

**The same ingestion as a notebook.** `spark/partner_stock_sync.ipynb` walks through it cell by cell: it reads the newest feed in `gs://$BUCKET/partner_feed/glowlab/`, starts a Serverless for Apache Spark session (runtime 3.0, Spark Connect, the `iceberg-spark-runtime-4.0_2.13` jar, the same catalog properties and the same `$SPARK_SA` service account as the batch), appends the feed to the Iceberg table and reads it back from BigQuery. Its ingest cell is the block between the `# --- shared with partner_stock_sync.ipynb ---` markers in `spark/partner_stock_sync.py`, the file Reset submits as the batch job; `tests/test_platform.py` fails if the two differ. Open it from the **Pyspark notebook** button on the Engineering tab, which imports it into Colab Enterprise from the fork's `main` branch. Run the cells top to bottom; the session takes about a minute to start, and the last cell stops it. Set `SPARK_NOTEBOOK_URL` to a raw `.ipynb` URL to import a different copy, or to a Colab Enterprise console URL (`https://console.cloud.google.com/...`) to open a notebook you already saved in your project as is. After you change `partner_stock_sync.py`, re-upload it with `gcloud storage cp spark/partner_stock_sync.py gs://$BUCKET/spark/`.

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
    sqlExpression: "seller_id = 'MKT-S003' OR partner_snapshot_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)"
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

## 5. Run the storefront demo tabs

Set `LAKEHOUSE_BUCKET=$BUCKET` in `.env`, or on Cloud Run with `gcloud run services update yeyo-storefront --update-env-vars LAKEHOUSE_BUCKET=$BUCKET`. After **Shop**, the storefront has five demo tabs. Open **Demo center** and press **Go viral**:

- **Demo center**: the Go viral / Stop / Reset demo controls; live shoppers, add-to-carts, orders, partner and store units left and waitlist, charted per 10 seconds; live vs. next-day batch report for the same orders; and **Proof: latest Pub/Sub events landed in BigQuery** (collapsed, shown while a spike runs): the newest `clickstream_events` rows with their Pub/Sub `message_id`, `publish_time` and `subscription_name`, plus events per minute.
- **Marketing**: the partner-stock gauge (from `vw_oversell_risk`, refreshed every 10 seconds) and the red oversell alert, which is also written to `oversell_alerts`; ad banners for each live campaign ("Hurry: only N left" during the surge, "Click & collect today" with the stores that have stock, the own-brand substitute once partner stock is gone), with the bids table below them. The viral alert and sold-out pop-ups appear on whichever tab is open.
- **Engineering**: **Pyspark notebook** opens the Glow Lab JSON ➜ Iceberg ingest (the same code Reset demo runs) and **Multi-Cloud** opens FitGear's BigLake table on S3 (section 9).
- **Governance**: **Lineage** opens `vw_oversell_risk` (Lineage tab), **Unified Governance** opens the Knowledge Catalog entry for the Iceberg table, and **Data Protection** queries `customer_master` as the storefront and shows the masked email and phone (section 10).
- **Talk to my Agent**: chat with the YeYo viral analyst data agent in plain English (section 6).

**Story flow (no clicks between beats except Go viral):**

1. Before the spike, the Marketing tab shows marketing's current (simulated) bids on other products.
2. **Go viral**: shoppers, carts and orders climb; the spike pauses automatically at `VIRAL_PAUSE_UNITS` partner units left (default 400, "Demo settings"). A **Viral alert** pop-up shows shoppers in the last minute, carts and time to sell-out; its button deploys high-bid Google Shopping, Search and social campaigns on the hero product.
3. Show the new campaign banners and bids on the Marketing tab. Coming back to Demo center resumes the spike.
4. When partner stock runs out, a **Sold out** pop-up lists the London stores with stock; the hero campaigns pause and high-bid "collect in store today" local inventory ads plus ads for the private label **YeYo Instant Eye Firming Gel** (`BEA-3004`) start. The spike carries on as click & collect orders from store stock (`VIRAL_STORE_UNITS`, default 400 per store, set in `stock_master_location` by **Reset demo**). The spike pauses again right there, before the first store order ("Pause again when partner stock sells out" in "Demo settings", `VIRAL_PAUSE_AT_SELL_OUT`, default on). Open the **Shop** tab to show the product taking click & collect orders only; switching back to Demo center resumes the spike.
5. When the stores sell out too, a **finish** pop-up compares units sold: Marketplace only (no lakehouse: the stores are never used) vs. Marketplace plus click & collect, about 3× with the defaults. On the **Shop** tab (online or marketplace channel) the product's **Add to cart** becomes **Click & collect**, which publishes an online order with `fulfillment_type = CLICK_AND_COLLECT` and the chosen store as `location_id`.

The simulator also sends a GA4-style `purchase` clickstream event with each order, so traffic source and postcode are attributed per session.

**Looker Studio (optional, alongside the storefront).** In Looker Studio, create a report with a BigQuery data source on `capstone_ds.vw_oversell_risk`. Then:

1. Add a scorecard for `partner_units`, a scorecard for `projected_units` and a table with `listing_id`, `risk_level`, `units_ordered_since_snapshot` and `partner_snapshot_ts`. Give `risk_level` conditional formatting: red when it equals `OVERSOLD`.
2. **Add data** > BigQuery > `capstone_ds.clickstream_events`, and add a time series of record count by `publish_time` (minute) broken down by `event_type`.
3. **Resource** > **Manage added data sources** > edit each source > **Data freshness** > every 1 minute (the BigQuery default is 12 hours). Press **Refresh data** during the demo for an immediate update.
4. Save the report and keep it open in a browser tab next to the storefront.

The Demo center polls BigQuery every 10 seconds; the report is at most a minute behind, so show the alert firing on the Marketing tab and the report as the dashboard marketing already uses on the same view.

Lineage: open `vw_oversell_risk` > **Lineage**. It shows `clickstream_events`, `marketplace_orders` and the Spark-written Iceberg table `partner_stock` feeding the view. Lineage shows up a few minutes after the view is created, so build it well before the demo.

**Reset demo** clears the run and submits the same Spark batch from the storefront, with the units set in "Partner units on hand". It takes about 3 minutes, and **Go viral** is disabled until the batch finishes. To let the Cloud Run service account submit it:

```
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SA --role=roles/dataproc.editor --condition=None
gcloud iam service-accounts add-iam-policy-binding $SPARK_SA --member=serviceAccount:$SA --role=roles/iam.serviceAccountUser
```

Then run the scan (`gcloud dataplex datascans run yeyo-oversell-dq --location=$LOCATION`) to show the rule failing on the oversold listing.

## 6. Talk to my Agent

The **Talk to my Agent** tab chats with **YeYo viral analyst**, a BigQuery data agent on the Conversational Analytics API (Gemini Data Analytics). Google hosts the model; you don't deploy an agent. The agent knows 12 `capstone_ds` tables and views (clickstream, online and marketplace orders, products, listings, sellers, `vw_oversell_risk`, `vw_partner_stock`, store stock and stores, oversell alerts, customers), a short YeYo system prompt and two example queries. You don't name tables: for each question it picks them, writes the SQL and runs it in BigQuery as the caller.

Create or update the agent and let the storefront's service account chat with it:

```
gcloud services enable geminidataanalytics.googleapis.com cloudaicompanion.googleapis.com
python -m storefront.data_agent $PROJECT_ID --user serviceAccount:$SA
```

The script grants Data Agent User on the agent only; the service account keeps its existing BigQuery roles. It also appears under **Agents** in BigQuery Studio, where you can chat with it as yourself.

Each answer shows the agent's text, a chart when one fits (ask for a chart, e.g. the first sample question), the result table, **SQL the agent ran in BigQuery** and **Open this job in BigQuery**, plus follow-up questions to click. **New conversation** clears the chat history the tab sends with each question.

Governance point: ask *"Show me 3 customers' emails"*. The agent queries `customer_master` as the storefront, a Masked Reader (section 10), so it only ever sees `XXXXX@domain` addresses.

## 7. 5-minute talk track

| Time | Beat | On screen |
| :-- | :-- | :-- |
| 0:00 | Hook: one unpaid TikTok | Storefront: FirmX listing from Glow Lab, "In stock" |
| 0:20 | Goes viral | Go viral: live chart climbing (Pub/Sub ➜ BigQuery in seconds) |
| 1:10 | Oversell caught | Marketing tab: red alert + gauge |
| 1:40 | CDO: can we trust it? | Governance tab **Lineage**: clickstream + orders + Spark Iceberg feed; **Unified Governance** and **Data Protection**: one catalog, masked PII |
| 2:10 | Head of Engineering: open formats | Engineering tab **Pyspark notebook**: JSON ➜ Iceberg files in your bucket ➜ same table in BigQuery; **Multi-Cloud**: FitGear on S3 |
| 2:40 | Marketing reacts | Marketing tab campaign banners: pause ads, collect in store, private label ads |
| 3:00 | Ads now point to stores | Spike pauses at sell-out; Shop tab: the product now takes click & collect only; back to Demo center resumes |
| 3:10 | Strong finish | Stores sell out too: Marketplace units vs. Marketplace + click & collect, about 3× |
| 3:20 | LOB: ask it yourself | Talk to my Agent: "Which London stores still have Instant FirmX Temporary Eye Tightener in stock?" and the SQL it ran |
| 3:30 | Next-day contrast | Live vs. next-day chart; query in place day one, move only what must be real time |
| 4:30 | Close | "Same viral moment, same product. The only difference is whether your data arrives in minutes or tomorrow, and that's an architecture decision." |

## 8. Cleanup

```
gcloud dataplex datascans delete yeyo-oversell-dq --location=$LOCATION --quiet
gcloud pubsub subscriptions delete yeyo-clickstream-bq --quiet && gcloud pubsub topics delete yeyo-clickstream --quiet
gcloud biglake iceberg catalogs delete yeyo_lake --quiet
gcloud storage rm -r gs://$BUCKET
```

If you ran section 9, remove the catalog tags with `gcloud dataplex aspect-types delete yeyo-partner-feed --location=global --quiet` after deleting the tables.

## 9. Optional: a second partner on Amazon S3 (BigQuery Omni)

Glow Lab's feed stays on Google Cloud, written by Spark into the Lakehouse Iceberg table. A second partner, FitGear Direct (`MKT-S003`, listings LST-0020/21/22/24), runs on AWS: its stock is an Apache Iceberg table that Amazon Athena writes in their S3 bucket. BigQuery Omni reads it in place, and only FitGear's filtered rows cross clouds. Nothing copies FitGear's whole table into Google Cloud:

```
GCS  yeyo_lake.yeyo_partner_lake.partner_stock  (Iceberg, written by Spark: Glow Lab)
  └─► vw_partner_stock ─► vw_oversell_risk ─► oversell_alerts
S3   s3://yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID/iceberg/partner_stock/  (Iceberg, written by Athena)
  └─ BigLake table yeyo_partner_aws.fitgear_stock (aws-us-east-1, query runs in AWS)
       └─ Omni low-stock query ─► yeyo_partner_east4.fitgear_low_stock (us-east4, result rows only)
```

Omni needs a billed project (not the sandbox) and runs on demand in `aws-us-east-1`. The low-stock query below reads the S3 table in place and saves only the result rows to `yeyo_partner_east4.fitgear_low_stock`, the Google Cloud region paired with `aws-us-east-1`. Dataplex lineage records each run in its `aws-us-east-1` location: S3 ➜ `fitgear_stock` ➜ `fitgear_low_stock`. The Marketing tab's `vw_partner_stock` covers Glow Lab only. The demo doesn't sell FitGear's products, so **Reset demo** only restocks Glow Lab (Spark) and the London stores. FitGear's snapshot then ages, so the `partner-snapshot-fresh` data quality rule skips `MKT-S003`.

1. Create the connection and datasets (`yeyo_partner_east4` holds the Omni results), and note the identity the connection prints:

```
bq mk --connection --connection_type=AWS --location=aws-us-east-1 \
  --iam_role_id=arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/yeyo-bq-omni-partner-stock yeyo_aws_partner
bq mk --dataset --location=aws-us-east-1 yeyo_partner_aws
bq mk --dataset --location=us-east4 yeyo_partner_east4
```

2. In the AWS console (us-east-1):
   - **S3 bucket:** create `yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID` with Block all public access on and SSE-S3 encryption.
   - **IAM policy:** create `yeyo-bq-omni-partner-stock-s3`. It allows `s3:ListBucket` on the bucket and `s3:GetObject` on `bucket/*` (BigQuery only reads).
   - **IAM role:** create `yeyo-bq-omni-partner-stock` with the Web identity trusted entity type, Google as the provider, and the identity from step 1 as the audience. Attach the policy. Then edit the trust policy so its condition is `"accounts.google.com:sub": "IDENTITY"`.
   - **Session length:** set the role's maximum session duration to 12 hours.
3. In Athena (AWS console, us-east-1), create FitGear's Iceberg table and load its stock. Set the query result location to any S3 folder first:

```sql
CREATE DATABASE IF NOT EXISTS fitgear;
```
```sql
CREATE TABLE fitgear.partner_stock (
  listing_id string, seller_id string, omni_item_id string, item_number string,
  warehouse_id string, units_on_hand int, snapshot_ts timestamp, source_system string)
LOCATION 's3://yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID/iceberg/partner_stock/'
TBLPROPERTIES ('table_type' = 'ICEBERG');
```
```sql
INSERT INTO fitgear.partner_stock VALUES
 ('LST-0020','MKT-S003','LIF-4001-OM','LIF-4001','FITGEAR-INDIANAPOLIS-DC',1500,current_timestamp,'fitgear-athena-iceberg'),
 ('LST-0021','MKT-S003','LIF-4002-OM','LIF-4002','FITGEAR-INDIANAPOLIS-DC',1500,current_timestamp,'fitgear-athena-iceberg'),
 ('LST-0022','MKT-S003','LIF-4003-OM','LIF-4003','FITGEAR-INDIANAPOLIS-DC',1500,current_timestamp,'fitgear-athena-iceberg'),
 ('LST-0024','MKT-S003','LIF-4005-OM','LIF-4005','FITGEAR-INDIANAPOLIS-DC',40,current_timestamp,'fitgear-athena-iceberg');
```

In S3, open `iceberg/partner_stock/metadata/` and note the newest `*.metadata.json` file name.

4. Point BigQuery at the Iceberg table and give the storefront its bucket name. Replace `LATEST.metadata.json` with the file name from step 3; re-run the first command after any Athena write:

```
bq query --use_legacy_sql=false --location=aws-us-east-1 "CREATE OR REPLACE EXTERNAL TABLE \`$PROJECT_ID.yeyo_partner_aws.fitgear_stock\` WITH CONNECTION \`$PROJECT_ID.aws-us-east-1.yeyo_aws_partner\` OPTIONS(format='ICEBERG', uris=['s3://yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID/iceberg/partner_stock/metadata/LATEST.metadata.json'])"
gcloud run services update yeyo-storefront --region us-central1 \
  --update-env-vars AWS_S3_BUCKET=yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID
```

Then create the Omni result table once, so the catalog step below can tag it (re-run it live during the demo):

```
bq query --use_legacy_sql=false --location=aws-us-east-1 "CREATE OR REPLACE TABLE \`$PROJECT_ID.yeyo_partner_east4.fitgear_low_stock\` AS SELECT listing_id, item_number, units_on_hand, snapshot_ts FROM \`$PROJECT_ID.yeyo_partner_aws.fitgear_stock\` WHERE seller_id = 'MKT-S003' AND units_on_hand < 100"
```

FitGear's four listings have 1,500 units each, and 40 for LST-0024, which is LOW_STOCK. The **Multi-Cloud** button on the Engineering tab opens the S3 table: click **Query** to run Omni from BigQuery Studio, or **Lineage** for the AWS chain. To drop the AWS feed, unset `AWS_S3_BUCKET`.

### One catalog for both clouds

Dataplex Universal Catalog (shown in the console as Knowledge Catalog) already indexes the BigQuery Omni tables on S3 next to the Google Cloud tables. To make them one list, tag every partner stock table with the same aspect, `yeyo-partner-feed`. Its fields are partner, seller, cloud, storage, table format, writer and refresh. The script creates the aspect type, then tags the Glow Lab Iceberg table (Spark on Google Cloud), FitGear's S3 Iceberg table (AWS), the Omni result table in `yeyo_partner_east4`, and `vw_partner_stock`. It skips any table you haven't created:

```
bash catalog/tag_partner_feeds.sh YOUR_PROJECT_ID
```

The first tags can take a minute while the new aspect type propagates (the script retries), and search picks them up a minute or two later. Then search Knowledge Catalog for:

- `aspect:yeyo-partner-feed`: every partner stock asset, whichever cloud or engine holds it.
- `aspect:yeyo-partner-feed.cloud=AWS`: only the tables whose data stays in Amazon S3.
- `fitgear`: FitGear Direct's AWS table, `fitgear_stock` (Iceberg). Its entry shows where the files live in Amazon S3 (the source URI `s3://yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID/iceberg/partner_stock/metadata/...metadata.json`).

Open any result and the **YeYo partner feed** aspect shows where the data lives and who writes it. Re-run the script after recreating a table; it overwrites the tags.

## 10. Governed catalog and data protection

A second aspect, `yeyo-governed-asset` (domain, cloud, storage, sensitivity, access control, owner), tags orders, clickstream, both partner feeds, FitGear's S3 table, alerts and `customer_master`. `customer_master.email` and `phone_number` get policy tags in a `YeYo governance` taxonomy with masking rules (email mask, last four characters). The storefront and data agent service accounts are Masked Readers, and the presenter is a Fine-Grained Reader:

```
gcloud services enable datacatalog.googleapis.com bigquerydatapolicy.googleapis.com
SA=yeyo-storefront-sa@$PROJECT_ID.iam.gserviceaccount.com
for R in roles/datalineage.viewer roles/dataplex.catalogViewer; do
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SA --role=$R --condition=None
done
gcloud storage buckets add-iam-policy-binding gs://$BUCKET --member=serviceAccount:$SA --role=roles/storage.objectCreator
gcloud storage cp spark/partner_stock_sync.py gs://$BUCKET/spark/
AWS_S3_BUCKET=yeyo-glowlab-partner-YOUR_AWS_ACCOUNT_ID python -m storefront.governance $PROJECT_ID --presenter YOUR_EMAIL
```

Leave out `AWS_S3_BUCKET` if you skipped section 9. A Knowledge Catalog search for `aspect:yeyo-governed-asset` should list all 10 assets. **Data Protection** on the Governance tab returns `XXXXX@bennett.com` and `XXXXX5632`; the same query in BigQuery Studio, as the presenter, returns the real values.


Cleanup: `bq update --schema` without the policy tags or drop `customer_master`'s tags in **BigQuery ➜ Policy tags**, delete the `YeYo governance` taxonomy, and run `gcloud dataplex aspect-types delete yeyo-governed-asset --location=global --quiet`.
