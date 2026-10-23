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

import re
from pathlib import Path

from storefront.events import CartLine, build_order_event
from storefront.publisher import event_to_row

SQL = Path(__file__).resolve().parent.parent / "sql" / "streaming_tables.sql"


def _ddl_columns():
    body = SQL.read_text().split("CREATE TABLE IF NOT EXISTS capstone_ds.online_orders (", 1)[1]
    body = body.split("\n)\nPARTITION BY", 1)[0]
    top = set(re.findall(r"^  (\w+) ", body, flags=re.M))
    lines = set(re.findall(r"^    (\w+) ", body, flags=re.M))
    return top, lines


def test_landing_table_schema_matches_event_rows(catalog):
    p = catalog.products_for_channel("marketplace")[0]
    ev = build_order_event(catalog, [CartLine(p, 1, catalog.listings_for(p.omni_item_id)[0])], channel="marketplace")
    row = event_to_row(ev)
    top, lines = _ddl_columns()
    assert set(row) == top
    assert set(row["line_items"][0]) == lines
