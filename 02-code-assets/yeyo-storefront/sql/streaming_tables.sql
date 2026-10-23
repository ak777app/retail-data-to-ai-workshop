-- Copyright 2025 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Module-11 landing tables for YeYo order events (topic: yeyo-orders).
-- One row per order; line items are a nested repeated field.
-- The last four columns are written by the BigQuery subscription (--write-metadata)
-- or by the storefront's BigQuery-direct fallback.

CREATE TABLE IF NOT EXISTS capstone_ds.online_orders (
  event_id STRING,
  event_type STRING,
  event_source STRING,
  schema_version STRING,
  spike_id STRING,
  order_id STRING,
  order_ts TIMESTAMP,
  channel STRING,
  customer_id STRING,
  location_id STRING,
  fulfillment_type STRING,
  marketplace_seller_id STRING,
  order_status STRING,
  payment_type STRING,
  currency STRING,
  item_count INT64,
  order_total NUMERIC,
  line_items ARRAY<STRUCT<
    line_number INT64,
    item_number STRING,
    omni_item_id STRING,
    description STRING,
    category STRING,
    quantity INT64,
    unit_price NUMERIC,
    line_item_total NUMERIC,
    marketplace_seller_id STRING,
    marketplace_listing_id STRING,
    fulfillment_type STRING
  >>,
  subscription_name STRING,
  message_id STRING,
  publish_time TIMESTAMP,
  attributes STRING
)
PARTITION BY DATE(publish_time)
CLUSTER BY channel, event_source
OPTIONS (description = 'YeYo online orders streamed from Pub/Sub topic yeyo-orders');

CREATE TABLE IF NOT EXISTS capstone_ds.marketplace_orders
LIKE capstone_ds.online_orders
OPTIONS (description = 'YeYo marketplace orders streamed from Pub/Sub topic yeyo-orders');

CREATE TABLE IF NOT EXISTS capstone_ds.instore_orders
LIKE capstone_ds.online_orders
OPTIONS (description = 'YeYo in-store (POS) orders streamed from Pub/Sub topic yeyo-orders');

CREATE OR REPLACE VIEW capstone_ds.vw_omni_channel_orders AS
SELECT * FROM capstone_ds.online_orders
UNION ALL
SELECT * FROM capstone_ds.marketplace_orders
UNION ALL
SELECT * FROM capstone_ds.instore_orders;

CREATE OR REPLACE VIEW capstone_ds.vw_omni_channel_order_lines AS
SELECT
  o.order_id,
  o.order_ts,
  o.publish_time,
  o.channel,
  o.event_source,
  o.spike_id,
  o.customer_id,
  o.location_id,
  o.fulfillment_type,
  li.line_number,
  li.item_number,
  li.omni_item_id,
  li.description,
  li.category,
  li.quantity,
  li.unit_price,
  li.line_item_total,
  li.marketplace_seller_id,
  li.marketplace_listing_id,
  li.fulfillment_type AS line_fulfillment_type
FROM capstone_ds.vw_omni_channel_orders AS o, UNNEST(o.line_items) AS li;
