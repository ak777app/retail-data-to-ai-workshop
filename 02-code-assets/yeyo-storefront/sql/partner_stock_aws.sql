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


-- Optional (Module-13 section 7): Glow Lab's partner stock lives in Amazon S3. BigQuery Omni
-- writes it there and reads it through yeyo_partner_aws.partner_stock_s3 (aws-us-east-1); a
-- cross-cloud CTAS lands Glow Lab's rows in yeyo_partner_east4 (us-east4) and a table copy
-- brings them next to the clickstream. Run after sql/viral_moment.sql, in us-central1.
-- Replace PROJECT_ID before running.

CREATE TABLE IF NOT EXISTS capstone_ds.partner_stock_aws_replica (
  listing_id STRING, seller_id STRING, omni_item_id STRING, item_number STRING,
  warehouse_id STRING, units_on_hand INT64, snapshot_ts TIMESTAMP, source_system STRING
)
OPTIONS (description = 'Glow Lab partner stock copied from Amazon S3 (BigQuery Omni) by Reset demo');

CREATE OR REPLACE VIEW capstone_ds.vw_partner_stock AS
SELECT listing_id, seller_id, omni_item_id, item_number, warehouse_id, units_on_hand, snapshot_ts, source_system
FROM capstone_ds.partner_stock_aws_replica;
