# YeYo end-to-end demo on a Google Cloud trial account

This runbook tells one continuous story for **YeYo**, a pharmacy and lifestyle retailer that sells in stores, online and through third-party sellers on the YeYo Marketplace. It starts at the YeYo storefront, where a shopper places an order or the presenter triggers an order spike. The event streams into BigQuery, flows through warehousing, the Iceberg/BigLake lakehouse, governance and BI, and finally the agents act on it.

```
 1. YeYo storefront UI ──► 2. Pub/Sub yeyo-orders ──► BigQuery capstone_ds.{online,marketplace,instore}_orders
   (order / spike)            (BigQuery subscriptions)                 │
                                                                       ▼
            3. Warehousing ─► 4. Iceberg / BigLake ─► 5. Governance ─► 6. BI / Conversational Analytics ─► 7. Agents
```

<hr>

## Variables used throughout

Set these once in Cloud Shell. Every module and the storefront `.env` use the same names.

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID --format="value(projectNumber)"`
LOCATION="us-central1"

BQ_DATASET="capstone_ds"
PUBSUB_TOPIC="yeyo-orders"
BQ_ONLINE_ORDERS_TABLE="online_orders"
BQ_MARKETPLACE_ORDERS_TABLE="marketplace_orders"
BQ_INSTORE_ORDERS_TABLE="instore_orders"
```

| Asset | Name |
| :-- | :-- |
| Pub/Sub topic | `yeyo-orders` |
| BigQuery subscriptions | `yeyo-online-orders-bq`, `yeyo-marketplace-orders-bq`, `yeyo-instore-orders-bq` (filtered on attribute `channel`) |
| Landing tables | `capstone_ds.online_orders`, `capstone_ds.marketplace_orders`, `capstone_ds.instore_orders` |
| Omni-channel views | `capstone_ds.vw_omni_channel_orders` (one row per order), `capstone_ds.vw_omni_channel_order_lines` (one row per line, keyed by `omni_item_id`) |
| Storefront | `02-code-assets/yeyo-storefront` (Cloud Shell, or the optional Cloud Run service `yeyo-storefront`) |

<hr>

## Step 0 - Trial-account setup (10 minutes)

1. Create or select a project on the trial account, then open Cloud Shell.
2. Clone the repo and set the variables above:
   ```
   git clone https://github.com/ak777app/retail-data-to-ai-workshop.git
   ```
3. Enable the minimum APIs for steps 1-2:
   ```
   gcloud services enable pubsub.googleapis.com bigquery.googleapis.com --project=$PROJECT_ID
   ```
   The later steps enable their own APIs in their modules ([Module 0](03-lab-manuals/Module-00-Provisioning.md) lists all of them).

<hr>

## Step 1 - Start at the YeYo storefront

**Modules:** [Module 11](03-lab-manuals/Module-11-Realtime-Streaming-To-BigQuery.md) (sections 1-5, about 10 minutes) and [Module 12](03-lab-manuals/Module-12-YeYo-Storefront-UI.md) (sections 1-3).

1. Create the topic, landing tables and BigQuery subscriptions (Module 11).
2. Run the storefront from Cloud Shell and open **Web Preview on port 8080** (Module 12):
   ```
   cd ~/retail-data-to-ai-workshop/02-code-assets/yeyo-storefront
   cp .env.example .env && sed -i "s/YOUR_PROJECT_ID/${PROJECT_ID}/" .env
   pip install -r requirements.txt
   streamlit run app.py --server.port=8080 --server.headless=true
   ```
3. Tell the story. A shopper buys allergy relief **online**, then orders a yoga mat from a **marketplace seller**. Show the order event JSON: header, line items, `channel`, `marketplace_seller_id`, `marketplace_listing_id` and `omni_item_id`.
4. Then open **Ops: viral moment** for the spike (below).

<hr>

**Viral-moment variant (5 minutes):** instead of the allergy spike, open **Ops: viral moment** and press **Go viral**. One marketplace cosmetic, Instant FirmX Temporary Eye Tightener from Glow Lab Beauty, gets TikTok-scale traffic: clickstream and orders stream in live, the Spark-written Iceberg partner feed runs out, and the oversell alert fires while the listing still says "in stock". Setup and talk track: [Module 13](03-lab-manuals/Module-13-YeYo-Viral-Moment.md).

<hr>

## Step 2 - Watch the events stream into BigQuery

**Module:** [Module 12 section 4-5](03-lab-manuals/Module-12-YeYo-Storefront-UI.md#4-watch-rows-arrive-in-bigquery)

Run this per-minute count in the BigQuery console while the spike is running. You can also click **Refresh from BigQuery** in the storefront.

```
SELECT TIMESTAMP_TRUNC(publish_time, MINUTE) AS minute, channel, event_source, COUNT(*) AS orders
FROM `YOUR_PROJECT_ID.capstone_ds.vw_omni_channel_orders`
WHERE publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
GROUP BY minute, channel, event_source
ORDER BY minute DESC, channel;
```

The point to land is that there is no pipeline code: Pub/Sub BigQuery subscriptions write each order within seconds. If Pub/Sub isn't available, the storefront falls back to writing directly through the BigQuery client.

<hr>

## Step 3 - Data warehousing

**Modules:** [Module 20](03-lab-manuals/Module_20_Capstone_Setup.md) (loads the rest of `capstone_ds`), then [Module 3](03-lab-manuals/Module-03-Data-Engineering-Agent-For-Warehousing.md) and [Module 4](03-lab-manuals/Module-04-Data-Engineering-Agent-For-Reporting.md).

Present the streamed orders as another fact source next to the POS history. `vw_omni_channel_order_lines` already has one row per line keyed on `omni_item_id`, the same key as `product_master`. When you prompt the Data Engineering Agent, include it as a source of the sales fact, for example:<br>
"Add online and marketplace orders from vw_omni_channel_order_lines to the sales fact with a channel dimension and a marketplace seller dimension."

<hr>

## Step 4 - Iceberg / BigLake lakehouse

**Module:** [Module 10](03-lab-manuals/Module-10-Apache-Iceberg-Lakehouse.md)

Follow Module 10 to create the BigLake connection, the bucket and the lakehouse dataset `rscw_lh_ds`. Then snapshot the streamed order lines into a BigQuery managed Iceberg table so Spark (Dataproc Serverless) can read the freshest omni-channel orders. Replace the placeholders with the connection and bucket from Module 10:

```
CREATE TABLE `YOUR_PROJECT_ID.rscw_lh_ds.yeyo_order_lines_iceberg`
WITH CONNECTION `YOUR_PROJECT_ID.us-central1.YOUR_BIGLAKE_CONNECTION`
OPTIONS (file_format = 'PARQUET', table_format = 'ICEBERG',
         storage_uri = 'gs://YOUR_LAKEHOUSE_BUCKET/yeyo_order_lines_iceberg')
AS SELECT * FROM `YOUR_PROJECT_ID.capstone_ds.vw_omni_channel_order_lines`;
```

<hr>

## Step 5 - Governance

**Modules:** [Module 2a](03-lab-manuals/Module-02a-Data-Insights-API.md), [Module 2b](03-lab-manuals/Module-02b-Data-Insights-API.md), [Module 2c](03-lab-manuals/Module-02c-Data-Insights-API.md)

Profile the landing tables and generate data insights (descriptions, relationships and golden queries) for `online_orders` and `marketplace_orders`. The `omni_item_id` relationship to `product_master` is what grounds the agents later on.

<hr>

## Step 6 - BI and conversational analytics

**Modules:** [Module 5](03-lab-manuals/Module-05-Conversational-Analytics.md), [Module 6](03-lab-manuals/Module-06-EDA-with-Data-Science-Agent.md), [Module 7](03-lab-manuals/Module-07-Forecasting-WithTimesFM.md)

Add `vw_omni_channel_orders` and `vw_omni_channel_order_lines` to the data agent and ask questions such as:
- "How many orders per minute did YeYo receive in the last 30 minutes, by channel?"
- "Which marketplace sellers sold the most allergy products during the spike?"

Then forecast item demand that includes the spike with TimesFM (Module 7).

<hr>

## Step 7 - Agents act on it

**Modules:** [Module 21](03-lab-manuals/Module_21_ADK_Primer_Data_Analyst_Agent.md) to [Module 25](03-lab-manuals/Module_25_Logistics_Manager_Agent.md)

The storyline: the demand planner sees the spike in online and marketplace demand for the top `omni_item_id`s. The inventory manager checks stock, the procurement manager raises purchase orders, and the logistics manager plans the shipments. A good first prompt in `adk web` is:<br>
"Which items had unusual online or marketplace demand in the last hour, and do we have enough stock?"

<hr>

## Trial-account cost and quota summary

- Steps 1-2 cost effectively nothing: a 200 to 2,000 order spike is about 0.2-2 MB through Pub/Sub, well inside the free tiers. The spike is capped by `SPIKE_MAX_ORDERS` (2,000) and `SPIKE_MAX_DURATION_SECONDS` (600).
- Keep a `publish_time` filter on the landing tables, which are day-partitioned.
- Cloud Run deployment of the storefront is optional. If you deploy it, use `--min-instances 0 --max-instances 1` and delete it after the demo.
- The later steps (AlloyDB, Dataproc, Vertex AI and agents) are the main cost drivers. See each module's notes and clean up afterwards.

## Cleanup

See [Module 11 section 9](03-lab-manuals/Module-11-Realtime-Streaming-To-BigQuery.md#9-cleanup) and [Module 12 section 6](03-lab-manuals/Module-12-YeYo-Storefront-UI.md#6-optional-deploy-to-cloud-run), plus the cleanup section of each module you ran.
