# Module 12: YeYo storefront UI - place mock orders and simulate an order spike

<hr>

## Motivation:

Every YeYo demo starts here. The storefront is a small [Streamlit](https://streamlit.io) app with a YeYo pharmacy and lifestyle catalog, a cart, and a channel toggle for in-store, online, or the YeYo Marketplace. For marketplace items you also pick the third-party seller. When you place an order, the app builds an order event (header plus line items, linked by `omni_item_id`) and publishes it to the `yeyo-orders` Pub/Sub topic from [Module 11](Module-11-Realtime-Streaming-To-BigQuery.md). BigQuery subscriptions then land the event in `capstone_ds.online_orders` / `capstone_ds.marketplace_orders` within seconds.

The **Simulate order spike** button publishes a burst of synthetic online and marketplace orders through the same publisher and event schema. This gives the later modules a demand spike to work with: warehousing, Iceberg/BigLake, governance, BI and agents. See [DEMO-TRIAL.md](../DEMO-TRIAL.md) for the full narrative.

Code: [`02-code-assets/yeyo-storefront`](../02-code-assets/yeyo-storefront)

| File | Purpose |
| :-- | :-- |
| `app.py` | Streamlit UI (Shop, Simulate order spike, Orders & BigQuery landing tabs) |
| `storefront/catalog.py` | Loads `product_master`, marketplace sellers and listings from BigQuery or `sample_data/` |
| `storefront/events.py` | Order event builder shared by single orders and the spike |
| `storefront/publisher.py` | Pub/Sub publisher, BigQuery-direct fallback, and a local-file mode for rehearsals |
| `storefront/spike.py` | Background spike generator with live progress |
| `sql/streaming_tables.sql` | Landing tables and views (used in Module 11) |

<hr>

## Duration:

15 minutes, plus 1-10 minutes per spike.

<hr>

## Prerequisites:

1. [Module 11](Module-11-Realtime-Streaming-To-BigQuery.md) completed, giving you the topic, BigQuery subscriptions and landing tables. You can skip it to try the UI with `PUBLISH_MODE="local"`.
2. Cloud Shell, or any machine with Python 3.10+ and `gcloud auth application-default login`.

<hr>

## Table of contents

| # | Section focus |
| -- | :--- |
| 1 | [Configure the storefront (.env)](Module-12-YeYo-Storefront-UI.md#1-configure-the-storefront-env) |
| 2 | [Run the storefront in Cloud Shell](Module-12-YeYo-Storefront-UI.md#2-run-the-storefront-in-cloud-shell) |
| 3 | [Place mock online and marketplace orders](Module-12-YeYo-Storefront-UI.md#3-place-mock-online-and-marketplace-orders) |
| 4 | [Watch rows arrive in BigQuery](Module-12-YeYo-Storefront-UI.md#4-watch-rows-arrive-in-bigquery) |
| 5 | [Simulate an order spike](Module-12-YeYo-Storefront-UI.md#5-simulate-an-order-spike) |
| 6 | [(Optional) Deploy to Cloud Run](Module-12-YeYo-Storefront-UI.md#6-optional-deploy-to-cloud-run) |
| 7 | [Trial-account cost and quota considerations](Module-12-YeYo-Storefront-UI.md#7-trial-account-cost-and-quota-considerations) |

<hr>

# Lab module

## 1. Configure the storefront (.env)

```
cd ~
git clone https://github.com/ak777app/retail-data-to-ai-workshop.git   # skip if already cloned
cd ~/retail-data-to-ai-workshop/02-code-assets/yeyo-storefront

cp .env.example .env
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
sed -i "s/YOUR_PROJECT_ID/${PROJECT_ID}/" .env
cat .env
```

The variable names are the same as in Module 11:

| Variable | Default | Meaning |
| :-- | :-- | :-- |
| `PROJECT_ID` | - | Your project |
| `LOCATION` | `us-central1` | BigQuery location |
| `BQ_DATASET` | `capstone_ds` | Dataset holding the landing tables (and, optionally, the catalog) |
| `PUBSUB_TOPIC` | `yeyo-orders` | Topic from Module 11 |
| `BQ_ONLINE_ORDERS_TABLE` / `BQ_MARKETPLACE_ORDERS_TABLE` / `BQ_INSTORE_ORDERS_TABLE` | `online_orders` / `marketplace_orders` / `instore_orders` | Landing tables |
| `CATALOG_SOURCE` | `sample` | `sample` reads the bundled CSVs, `bigquery` reads `product_master` / `marketplace_seller_master` / `marketplace_listings`, and `auto` tries BigQuery first, then sample |
| `PUBLISH_MODE` | `auto` | `auto` tries Pub/Sub first, then BigQuery direct. The other values are `pubsub`, `bigquery` and `local` (writes a JSONL file, no GCP needed) |
| `SPIKE_DEFAULT_ORDERS` / `SPIKE_MAX_ORDERS` | `200` / `2000` | Spike size default and cap |
| `SPIKE_DEFAULT_DURATION_SECONDS` / `SPIKE_MAX_DURATION_SECONDS` | `60` / `600` | Burst window default and cap |

How the fallback works: in `auto` mode, if the Pub/Sub client can't be created or a publish fails (for example the topic is missing or there's no permission), the storefront switches to the BigQuery client and calls `insert_rows_json` on the same landing tables. Rows written this way have the same columns, with `subscription_name = "bigquery-direct-fallback"`. The sidebar shows which path is active.

<hr>

## 2. Run the storefront in Cloud Shell

```
cd ~/retail-data-to-ai-workshop/02-code-assets/yeyo-storefront
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

gcloud auth application-default login      # Cloud Shell: only needed if client libraries can't find credentials
streamlit run app.py --server.port=8080 --server.headless=true
```

In Cloud Shell, click **Web Preview > Preview on port 8080**.<br>
Check the sidebar under **Streaming target**. It shows your project, the publisher (`Pub/Sub projects/<PROJECT_ID>/topics/yeyo-orders` with BigQuery direct fallback) and the catalog source.

<hr>

## 3. Place mock online and marketplace orders

**Online order**
1. In the sidebar, set **Channel** to **Online** and pick a shopper.
2. On the **Shop** tab, filter by category or search (e.g. `allergy`), set a quantity and click **Add to cart**.
3. Click **Place order**. A confirmation shows the `order_id` and the Pub/Sub message ID; expand **Order event JSON** to see the exact payload that was published.

**Marketplace order**
1. Switch **Channel** to **Marketplace**. Switching channels clears the cart, and the grid now shows only marketplace-eligible products, each with a **Seller** selector. A seller's price can differ from YeYo's list price.
2. Pick a seller, add to cart, then **Place order**. Each line item carries `marketplace_seller_id` and `marketplace_listing_id` along with `omni_item_id`. `fulfillment_type` is `YEYO_FULFILLED` or `SELLER_FULFILLED`, depending on the seller.

In-store orders work too: pick **In-store** and a store. They land in `instore_orders`, so the omni-channel views cover all three channels.

<hr>

## 4. Watch rows arrive in BigQuery

On the **Orders & BigQuery landing** tab, click **Refresh from BigQuery** for a per-minute count. You can also run this in the BigQuery console:

```
SELECT publish_time, order_id, channel, fulfillment_type, marketplace_seller_id, order_total,
       ARRAY_LENGTH(line_items) AS lines
FROM `YOUR_PROJECT_ID.capstone_ds.vw_omni_channel_orders`
WHERE publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY publish_time DESC
LIMIT 20;
```

Line items joined back to the catalog through `omni_item_id`. This example assumes `product_master` is loaded in `capstone_ds`; otherwise use `description` and `category` from the event.

```
SELECT l.order_id, l.channel, l.omni_item_id, l.description, l.quantity, l.unit_price,
       l.marketplace_seller_id, l.marketplace_listing_id
FROM `YOUR_PROJECT_ID.capstone_ds.vw_omni_channel_order_lines` AS l
WHERE l.publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY l.publish_time DESC;
```

<hr>

## 5. Simulate an order spike

1. Open the **Simulate order spike** tab.
2. Set **Number of orders**, the **Burst window (seconds)** that the orders are spread across (0 publishes as fast as possible), and the **Marketplace share (%)**.
3. Click **Simulate order spike**. The spike runs in a background thread, so you can keep shopping. A live progress bar shows `Published X/N events`, the online and marketplace split, and events per second. **Stop** cancels the burst.
4. When it finishes, a summary shows the published and failed counts, elapsed time, simulated revenue and the `spike_id` (the online/marketplace split stays in the metrics above it).

Each synthetic order picks a random active product. Marketplace orders also pick a random active seller listing. Quantities are random from 1 to 3, with 1 to 3 lines per order, and `order_ts` values are spread across the burst window. Every event uses the same builder and publisher as a shopper's order, with `event_source = "spike_simulator"` and a shared `spike_id`.

**Suggested burst size for a trial account:** start with the default of **200 orders over 60 seconds**. For a dramatic demo, **1,000 orders over 120 seconds** is still around 1 MB of Pub/Sub traffic. The UI is capped at `SPIKE_MAX_ORDERS` (default 2,000) and `SPIKE_MAX_DURATION_SECONDS` (default 600); raise these in `.env` only if you need more.

**Watch the spike arrive in near real time.** Run this in the BigQuery console and re-run it every few seconds:

```
SELECT
  TIMESTAMP_TRUNC(publish_time, MINUTE) AS minute,
  channel,
  event_source,
  COUNT(*) AS orders,
  ROUND(SUM(order_total), 2) AS revenue
FROM `YOUR_PROJECT_ID.capstone_ds.vw_omni_channel_orders`
WHERE publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
GROUP BY minute, channel, event_source
ORDER BY minute DESC, channel;
```

To isolate one burst, use the `spike_id` from the summary:

```
SELECT channel, COUNT(*) AS orders, MIN(publish_time) AS first_row, MAX(publish_time) AS last_row,
       MIN(order_ts) AS first_order_ts, MAX(order_ts) AS last_order_ts
FROM `YOUR_PROJECT_ID.capstone_ds.vw_omni_channel_orders`
WHERE spike_id = 'SPIKE_ID_FROM_SUMMARY'
GROUP BY channel;
```

The top-selling items during the spike, which is a good handoff to the demand planner agent:

```
SELECT omni_item_id, description, SUM(quantity) AS units
FROM `YOUR_PROJECT_ID.capstone_ds.vw_omni_channel_order_lines`
WHERE event_source = 'spike_simulator'
  AND publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY omni_item_id, description
ORDER BY units DESC
LIMIT 10;
```

<hr>

## 6. (Optional) Deploy to Cloud Run

This step is optional; the demo runs fine from Cloud Shell. Deploy if you want a stable URL for presenters.

```
cd ~/retail-data-to-ai-workshop/02-code-assets/yeyo-storefront
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
LOCATION="us-central1"
SA="yeyo-storefront-sa@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com
gcloud iam service-accounts create yeyo-storefront-sa --display-name="YeYo storefront"
for ROLE in roles/pubsub.publisher roles/bigquery.dataEditor roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:${SA}" --role=$ROLE --condition=None
done

gcloud run deploy yeyo-storefront \
  --source . \
  --region $LOCATION \
  --service-account $SA \
  --no-allow-unauthenticated \
  --min-instances 0 --max-instances 1 \
  --timeout 3600 \
  --set-env-vars PROJECT_ID=$PROJECT_ID,LOCATION=$LOCATION,BQ_DATASET=capstone_ds,PUBSUB_TOPIC=yeyo-orders,CATALOG_SOURCE=auto,PUBLISH_MODE=auto

# Open it privately through an authenticated local proxy, then Web Preview on port 8080
gcloud run services proxy yeyo-storefront --region $LOCATION --port 8080
```

Notes:
- `.env` isn't uploaded (see `.gcloudignore`); configuration comes from `--set-env-vars`.
- `--max-instances 1` keeps all sessions, and any running spike, on one instance, and caps cost.
- Cleanup: `gcloud run services delete yeyo-storefront --region $LOCATION`.

<hr>

## 7. Trial-account cost and quota considerations

- **Pub/Sub and BigQuery subscriptions:** a 1 KB event times 2,000 orders is about 2 MB. That is well inside the Pub/Sub free tier (10 GiB a month) and the Storage Write API free tier.
- **BigQuery queries:** the landing tables are partitioned on `publish_time`, so keep a `publish_time` filter. Re-running the per-minute query during a demo scans kilobytes.
- **BigQuery-direct fallback:** this uses streaming inserts (`insert_rows_json`), which are billed per MB with a 1 KB minimum per row. A 2,000-order spike costs a fraction of a cent, but Pub/Sub is the preferred path.
- **Cloud Run (optional):** `--min-instances 0` scales to zero when idle. Delete the service after the demo.
- **Quotas:** the default cap of 2,000 orders and 600 seconds keeps a spike far below the Pub/Sub publish quota and the BigQuery streaming quota. The publisher batches up to 100 messages or 50 ms per request.
- **Rehearsing offline:** `PUBLISH_MODE="local"` writes events to `local_events.jsonl` and uses no cloud resources.

<hr>

This concludes the module. Continue the end-to-end story in [DEMO-TRIAL.md](../DEMO-TRIAL.md).

<hr>
