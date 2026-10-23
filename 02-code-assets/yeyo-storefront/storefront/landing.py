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

"""BigQuery query used to watch storefront orders land (also used in Module-12)."""

from typing import Dict, List

from .config import Config

LANDING_SQL = """
SELECT
  TIMESTAMP_TRUNC(publish_time, MINUTE) AS minute,
  channel,
  event_source,
  COUNT(*) AS orders,
  ROUND(SUM(order_total), 2) AS revenue
FROM (
  SELECT publish_time, channel, event_source, order_total FROM `{online}`
  UNION ALL
  SELECT publish_time, channel, event_source, order_total FROM `{marketplace}`
  UNION ALL
  SELECT publish_time, channel, event_source, order_total FROM `{instore}`
)
WHERE publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {minutes} MINUTE)
GROUP BY minute, channel, event_source
ORDER BY minute DESC, channel
"""


def landing_sql(config: Config, minutes: int = 30) -> str:
    return LANDING_SQL.format(
        online=config.fq_table(config.online_orders_table),
        marketplace=config.fq_table(config.marketplace_orders_table),
        instore=config.fq_table(config.instore_orders_table),
        minutes=int(minutes),
    )


def query_landing(config: Config, minutes: int = 30, client=None) -> List[Dict]:
    from google.cloud import bigquery

    client = client or bigquery.Client(project=config.project_id or None, location=config.location)
    return [dict(r.items()) for r in client.query(landing_sql(config, minutes)).result()]
