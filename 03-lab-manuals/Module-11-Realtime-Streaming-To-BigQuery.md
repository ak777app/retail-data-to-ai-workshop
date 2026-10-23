# Module 11: Real-time order streaming into BigQuery with Pub/Sub BigQuery subscriptions

<hr>

## Motivation:

YeYo sells pharmacy and lifestyle products in its stores, on its website, and through third-party sellers on the YeYo Marketplace. Until now every module in this workshop has used static batch files. In this module YeYo starts streaming orders: each order is published as one JSON event to a Pub/Sub topic, and Pub/Sub [BigQuery subscriptions](https://cloud.google.com/pubsub/docs/bigquery) write it straight into BigQuery tables. You don't need Dataflow or any code to do it.

This is the streaming pillar of the end-to-end demo. The [YeYo storefront UI (Module 12)](Module-12-YeYo-Storefront-UI.md) publishes to the topic you create here, both for single mock orders and for a simulated order spike. The tables you create here then feed the warehousing, Iceberg/BigLake, governance, BI and agent modules.

```
YeYo storefront (Module 12) ──► Pub/Sub topic: yeyo-orders
                                      │  (message attribute: channel)
                ┌─────────────────────┼──────────────────────┐
                ▼                     ▼                      ▼
   yeyo-online-orders-bq   yeyo-marketplace-orders-bq   yeyo-instore-orders-bq
     (filter: online)        (filter: marketplace)        (filter: in_store)
                ▼                     ▼                      ▼
 capstone_ds.online_orders  capstone_ds.marketplace_orders  capstone_ds.instore_orders
                └──────────► capstone_ds.vw_omni_channel_orders / _order_lines
```

<hr>

## Duration:

15 minutes or less.

<hr>

## Prerequisites:

1. [Module 0](Module-00-Provisioning.md) completed (project, authentication).
2. The `capstone_ds` BigQuery dataset from [Module 20](Module_20_Capstone_Setup.md). If you are only running the storefront demo, section 2 creates the dataset for you.

<hr>

## Table of contents

| # | Section focus |
| -- | :--- |
| 1 | [Declare variables](Module-11-Realtime-Streaming-To-BigQuery.md#1-declare-variables) |
| 2 | [Enable APIs and create the dataset](Module-11-Realtime-Streaming-To-BigQuery.md#2-enable-apis-and-create-the-dataset) |
| 3 | [Create the landing tables](Module-11-Realtime-Streaming-To-BigQuery.md#3-create-the-landing-tables) |
| 4 | [Grant the Pub/Sub service agent access to BigQuery](Module-11-Realtime-Streaming-To-BigQuery.md#4-grant-the-pubsub-service-agent-access-to-bigquery) |
| 5 | [Create the topic and BigQuery subscriptions](Module-11-Realtime-Streaming-To-BigQuery.md#5-create-the-topic-and-bigquery-subscriptions) |
| 6 | [Publish a test event and query it](Module-11-Realtime-Streaming-To-BigQuery.md#6-publish-a-test-event-and-query-it) |
| 7 | [Order event schema](Module-11-Realtime-Streaming-To-BigQuery.md#7-order-event-schema) |
| 8 | [Trial-account cost and quota notes](Module-11-Realtime-Streaming-To-BigQuery.md#8-trial-account-cost-and-quota-notes) |
| 9 | [Cleanup](Module-11-Realtime-Streaming-To-BigQuery.md#9-cleanup) |

<hr>

# Lab module

## 1. Declare variables

Run these in Cloud Shell. The names match the storefront's `.env` file in Module 12, so you can copy and paste between the two.

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID --format="value(projectNumber)"`
LOCATION="us-central1"

BQ_DATASET="capstone_ds"
PUBSUB_TOPIC="yeyo-orders"
BQ_ONLINE_ORDERS_TABLE="online_orders"
BQ_MARKETPLACE_ORDERS_TABLE="marketplace_orders"
BQ_INSTORE_ORDERS_TABLE="instore_orders"

PUBSUB_SERVICE_AGENT="service-${PROJECT_NBR}@gcp-sa-pubsub.iam.gserviceaccount.com"
```

<hr>

## 2. Enable APIs and create the dataset

```
gcloud services enable pubsub.googleapis.com bigquery.googleapis.com --project=$PROJECT_ID

# No-op if the dataset already exists (e.g. from Module 20)
bq --location=$LOCATION mk --dataset -f $PROJECT_ID:$BQ_DATASET
```

<hr>

## 3. Create the landing tables

Each order is one row with its line items nested as a repeated `line_items` record. The last four columns are written by the subscription because of `--write-metadata`. They are `subscription_name`, `message_id`, `publish_time` and `attributes`. The tables are partitioned on `publish_time` and clustered on `channel` and `event_source`.

The DDL lives in [`02-code-assets/yeyo-storefront/sql/streaming_tables.sql`](../02-code-assets/yeyo-storefront/sql/streaming_tables.sql). It creates the three tables and two convenience views:
- `vw_omni_channel_orders` is the union of all channels, one row per order.
- `vw_omni_channel_order_lines` has one row per line item, joinable to `product_master` on `omni_item_id`.

```
cd ~/retail-data-to-ai-workshop/02-code-assets/yeyo-storefront
bq query --project_id=$PROJECT_ID --location=$LOCATION --use_legacy_sql=false \
  "$(sed "s/capstone_ds\./${BQ_DATASET}./g" sql/streaming_tables.sql)"

bq ls $PROJECT_ID:$BQ_DATASET
```

<hr>

## 4. Grant the Pub/Sub service agent access to BigQuery

BigQuery subscriptions write as the Google-managed Pub/Sub service agent. The agent needs `roles/bigquery.dataEditor` on the dataset; this lab grants it at project level to keep things simple.

```
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${PUBSUB_SERVICE_AGENT}" \
  --role="roles/bigquery.dataEditor" --condition=None
```

The identity that runs the storefront (you in Cloud Shell, or the Cloud Run service account in Module 12) needs `roles/pubsub.publisher`. For the direct fallback it also needs `roles/bigquery.dataEditor`. Project owners already have both.

<hr>

## 5. Create the topic and BigQuery subscriptions

One topic carries every channel. Each subscription filters on the `channel` message attribute, so every event lands in exactly one table.

```
gcloud pubsub topics create $PUBSUB_TOPIC --project=$PROJECT_ID

for CH in online marketplace instore; do
  case $CH in
    online)      TABLE=$BQ_ONLINE_ORDERS_TABLE;      FILTER='attributes.channel = "online"' ;;
    marketplace) TABLE=$BQ_MARKETPLACE_ORDERS_TABLE; FILTER='attributes.channel = "marketplace"' ;;
    instore)     TABLE=$BQ_INSTORE_ORDERS_TABLE;     FILTER='attributes.channel = "in_store"' ;;
  esac
  gcloud pubsub subscriptions create yeyo-${CH}-orders-bq \
    --project=$PROJECT_ID \
    --topic=$PUBSUB_TOPIC \
    --bigquery-table=$PROJECT_ID:$BQ_DATASET.$TABLE \
    --use-table-schema \
    --write-metadata \
    --drop-unknown-fields \
    --message-filter="$FILTER"
done

gcloud pubsub subscriptions list --project=$PROJECT_ID --filter="topic:$PUBSUB_TOPIC" --format="table(name,bigqueryConfig.table,filter)"
```

Notes:
- `--use-table-schema` maps the top-level JSON keys of the message to table columns, and nested `line_items` to the repeated record.
- `--drop-unknown-fields` keeps the stream flowing if a future event version adds fields.
- You can't change a subscription filter after creation; to change it, delete and recreate the subscription.
- Optional: add `--dead-letter-topic=yeyo-orders-dlq` to capture messages that BigQuery rejects. This needs a second topic and extra IAM; see [dead-letter topics](https://cloud.google.com/pubsub/docs/handling-failures).

<hr>

## 6. Publish a test event and query it

```
gcloud pubsub topics publish $PUBSUB_TOPIC --project=$PROJECT_ID \
  --attribute=channel=online,event_type=order_placed,event_source=cli,schema_version=1.0 \
  --message='{"event_id":"cli-test-1","event_type":"order_placed","event_source":"cli","schema_version":"1.0","order_id":"cli-test-1","order_ts":"2025-01-01T12:00:00Z","channel":"online","customer_id":"cli","location_id":"WHE-IL-WH","fulfillment_type":"SHIP_FROM_WAREHOUSE","order_status":"PLACED","payment_type":"CREDIT_CARD","currency":"USD","item_count":1,"order_total":8.99,"line_items":[{"line_number":1,"item_number":"PHA-1001","omni_item_id":"PHA-1001-OM","description":"Acetaminophen 500 mg Caplets - 100 ct","category":"pharmacy","quantity":1,"unit_price":8.99,"line_item_total":8.99}]}'
```

After a few seconds:

```
bq query --project_id=$PROJECT_ID --location=$LOCATION --use_legacy_sql=false \
"SELECT publish_time, order_id, channel, event_source, order_total, ARRAY_LENGTH(line_items) AS lines
 FROM \`$PROJECT_ID.$BQ_DATASET.vw_omni_channel_orders\`
 ORDER BY publish_time DESC LIMIT 10"
```

If no row shows up, check the subscription in the console. Go to **Pub/Sub > Subscriptions > yeyo-online-orders-bq > Metrics**, or look for errors in Cloud Logging. The usual causes are missing IAM from section 4 or a schema mismatch.

<hr>

## 7. Order event schema

The storefront ([`storefront/events.py`](../02-code-assets/yeyo-storefront/storefront/events.py)) and the spike simulator emit the same `schema_version` `1.0` event:

| Field | Notes |
| :-- | :-- |
| `event_id`, `order_id` | UUIDs; `event_id` is also used as the BigQuery insert ID in fallback mode |
| `event_type` | `order_placed` |
| `event_source` | `storefront` (shopper order) or `spike_simulator` |
| `spike_id` | set for spike orders, so one burst can be isolated in SQL |
| `order_ts` | when the order was placed (spike orders are spread across the burst window) |
| `channel` | `in_store`, `online` or `marketplace`; also sent as a message attribute |
| `customer_id`, `location_id` | `location_id` is the store for in-store orders and the warehouse for online orders; it is null for marketplace orders |
| `fulfillment_type` | `IN_STORE`, `SHIP_FROM_WAREHOUSE`, `SELLER_FULFILLED` or `YEYO_FULFILLED`. A marketplace order is `YEYO_FULFILLED` only if every line is; each line also carries its own `fulfillment_type` |
| `marketplace_seller_id` | seller for single-seller marketplace orders |
| `order_status`, `payment_type`, `currency`, `item_count`, `order_total` | header values |
| `line_items[]` | `line_number`, `item_number`, `omni_item_id`, `description`, `category`, `quantity`, `unit_price`, `line_item_total`, `marketplace_seller_id`, `marketplace_listing_id`, `fulfillment_type` |

`omni_item_id` is the same omni-channel key used by `product_master`, so streamed orders join to the catalog and the capstone data.

Message attributes are `channel`, `event_type`, `event_source` and `schema_version`.

<hr>

## 8. Trial-account cost and quota notes

- Pub/Sub has a free tier of 10 GiB of throughput a month. A YeYo order event is about 1 KB, so even thousands of demo orders cost effectively nothing.
- BigQuery subscriptions use the Storage Write API. Its free tier covers the first 2 TiB a month, far more than this demo needs.
- The tables are partitioned by day. Filter on `publish_time` in queries so you only scan recent partitions.
- No always-on compute is created in this module.

<hr>

## 9. Cleanup

```
for CH in online marketplace instore; do gcloud pubsub subscriptions delete yeyo-${CH}-orders-bq --project=$PROJECT_ID --quiet; done
gcloud pubsub topics delete $PUBSUB_TOPIC --project=$PROJECT_ID --quiet
bq rm -f -t $PROJECT_ID:$BQ_DATASET.$BQ_ONLINE_ORDERS_TABLE
bq rm -f -t $PROJECT_ID:$BQ_DATASET.$BQ_MARKETPLACE_ORDERS_TABLE
bq rm -f -t $PROJECT_ID:$BQ_DATASET.$BQ_INSTORE_ORDERS_TABLE
```

<hr>

This concludes the module. Proceed to [Module 12: YeYo storefront UI](Module-12-YeYo-Storefront-UI.md).

<hr>
