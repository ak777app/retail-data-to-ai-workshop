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

-- Viral-moment demo (Module-13). Run AFTER the partner Spark job has created the
-- Apache Iceberg table yeyo_lake.yeyo_partner_lake.partner_stock in the Lakehouse
-- runtime catalog (Iceberg REST catalog). Replace PROJECT_ID before running.

-- Clickstream landing table for Pub/Sub topic yeyo-clickstream.
CREATE TABLE IF NOT EXISTS capstone_ds.clickstream_events (
  event_id STRING,
  event_type STRING,
  event_source STRING,
  schema_version STRING,
  event_ts TIMESTAMP,
  viral_id STRING,
  session_id STRING,
  customer_id STRING,
  channel STRING,
  page_type STRING,
  omni_item_id STRING,
  item_number STRING,
  description STRING,
  marketplace_listing_id STRING,
  marketplace_seller_id STRING,
  listing_price NUMERIC,
  listing_stock_status STRING,
  quantity INT64,
  referrer STRING,
  postal_code STRING,
  subscription_name STRING,
  message_id STRING,
  publish_time TIMESTAMP,
  attributes STRING
)
PARTITION BY DATE(publish_time)
CLUSTER BY omni_item_id, event_type
OPTIONS (description = 'YeYo clickstream (page views, add-to-carts, waitlist joins) streamed from Pub/Sub topic yeyo-clickstream');

-- Partner stock feed: the Spark-written Iceberg table. sql/partner_stock_aws.sql repoints
-- this view at the copy of the partner's S3 data (BigQuery Omni) instead.
CREATE OR REPLACE VIEW capstone_ds.vw_partner_stock AS
SELECT listing_id, seller_id, omni_item_id, item_number, warehouse_id, units_on_hand, snapshot_ts, source_system
FROM `PROJECT_ID.yeyo_lake.yeyo_partner_lake.partner_stock`;

-- Partner stock (last snapshot) minus YeYo demand since that snapshot.
-- The listing page keeps saying IN_STOCK; the risk level says otherwise.
CREATE OR REPLACE VIEW capstone_ds.vw_oversell_risk AS
WITH snap AS (
  SELECT * FROM capstone_ds.vw_partner_stock
  QUALIFY ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY snapshot_ts DESC) = 1
),
demand AS (
  SELECT li.marketplace_listing_id AS listing_id, SUM(li.quantity) AS units_since_snapshot
  FROM capstone_ds.marketplace_orders AS o, UNNEST(o.line_items) AS li
  JOIN snap ON snap.listing_id = li.marketplace_listing_id
  WHERE o.publish_time >= snap.snapshot_ts
  GROUP BY 1
),
clicks AS (
  SELECT
    marketplace_listing_id AS listing_id,
    ARRAY_AGG(listing_stock_status ORDER BY publish_time DESC LIMIT 1)[OFFSET(0)] AS listing_stock_status,
    COUNTIF(event_type = 'page_view') AS page_views_10m,
    SUM(IF(event_type = 'add_to_cart', quantity, 0)) AS cart_units_10m,
    COUNTIF(event_type = 'waitlist_join') AS waitlist_10m
  FROM capstone_ds.clickstream_events
  WHERE publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
  GROUP BY 1
)
SELECT
  snap.listing_id,
  snap.seller_id,
  snap.omni_item_id,
  snap.snapshot_ts AS partner_snapshot_ts,
  snap.units_on_hand AS partner_units,
  IFNULL(d.units_since_snapshot, 0) AS units_ordered_since_snapshot,
  snap.units_on_hand - IFNULL(d.units_since_snapshot, 0) AS projected_units,
  IFNULL(c.listing_stock_status, 'IN_STOCK') AS listing_stock_status,
  IFNULL(c.page_views_10m, 0) AS page_views_10m,
  IFNULL(c.cart_units_10m, 0) AS cart_units_10m,
  IFNULL(c.waitlist_10m, 0) AS waitlist_10m,
  CASE
    WHEN snap.units_on_hand - IFNULL(d.units_since_snapshot, 0) <= 0 THEN 'OVERSOLD'
    WHEN snap.units_on_hand - IFNULL(d.units_since_snapshot, 0) < IFNULL(c.cart_units_10m, 0) THEN 'AT_RISK'
    ELSE 'OK'
  END AS risk_level
FROM snap
LEFT JOIN demand AS d USING (listing_id)
LEFT JOIN clicks AS c USING (listing_id);

-- Marketing view of the viral moment: traffic source, funnel, revenue and revenue at risk
-- (waitlist) by postcode, the YeYo store in the same ZIP3 area with stock, and a next best action.
CREATE OR REPLACE VIEW capstone_ds.vw_marketing_viral AS
WITH clicks AS (
  SELECT
    viral_id,
    marketplace_listing_id AS listing_id,
    omni_item_id,
    referrer,
    postal_code,
    COUNT(DISTINCT session_id) AS sessions,
    COUNTIF(event_type = 'page_view') AS page_views,
    SUM(IF(event_type = 'add_to_cart', quantity, 0)) AS cart_units,
    COUNTIF(event_type = 'purchase') AS purchase_sessions,
    SUM(IF(event_type = 'purchase', quantity, 0)) AS purchase_units,
    SUM(IF(event_type = 'purchase', quantity * listing_price, 0)) AS revenue,
    SUM(IF(event_type = 'waitlist_join', quantity, 0)) AS waitlist_units,
    SUM(IF(event_type = 'waitlist_join', quantity * listing_price, 0)) AS revenue_at_risk,
    MAX(publish_time) AS last_event_ts
  FROM capstone_ds.clickstream_events
  WHERE publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  GROUP BY viral_id, listing_id, omni_item_id, referrer, postal_code
),
stores AS (
  SELECT s.omni_item_id, SUBSTR(CAST(l.zip_code AS STRING), 1, 3) AS zip3, l.location_name, s.quantity_on_hand
  FROM capstone_ds.stock_master_location AS s
  JOIN capstone_ds.location_master AS l USING (location_id)
  WHERE l.location_type = 'Store'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY s.omni_item_id, s.location_id ORDER BY s.stock_date DESC) = 1
),
nearest AS (
  SELECT omni_item_id, zip3,
         ARRAY_AGG(STRUCT(location_name, quantity_on_hand) ORDER BY quantity_on_hand DESC LIMIT 1)[OFFSET(0)] AS store
  FROM stores
  WHERE quantity_on_hand > 0
  GROUP BY omni_item_id, zip3
)
SELECT
  c.*,
  r.risk_level,
  n.store.location_name AS nearest_store,
  n.store.quantity_on_hand AS nearest_store_units,
  CASE
    WHEN IFNULL(r.risk_level, 'OK') = 'OK' THEN 'Scale: raise bids while CPCs are low'
    WHEN n.store.location_name IS NOT NULL THEN CONCAT('Geo-target "collect in store today" at ', n.store.location_name)
    ELSE 'Swap creative to the own-brand substitute'
  END AS next_best_action
FROM clicks AS c
LEFT JOIN capstone_ds.vw_oversell_risk AS r USING (listing_id)
LEFT JOIN nearest AS n ON n.omni_item_id = c.omni_item_id AND n.zip3 = SUBSTR(c.postal_code, 1, 3);

-- Alert log written by the storefront Ops tab (INSERT ... SELECT from the view),
-- so Dataplex lineage records clickstream + orders + partner Iceberg -> alerts.
CREATE TABLE IF NOT EXISTS capstone_ds.oversell_alerts (
  alert_ts TIMESTAMP,
  viral_id STRING,
  listing_id STRING,
  seller_id STRING,
  omni_item_id STRING,
  risk_level STRING,
  listing_stock_status STRING,
  partner_units INT64,
  projected_units INT64,
  cart_units_10m INT64
)
PARTITION BY DATE(alert_ts)
OPTIONS (description = 'Oversell alerts raised when a listing says IN_STOCK but partner stock cannot cover demand');
