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

"""Streamed storefront orders must join to the Module 20 capstone tables."""

import csv
from pathlib import Path

import pytest

CAPSTONE = Path(__file__).resolve().parents[3] / "01-data-assets" / "capstone_data_1" / "fridge_tabular_data"
SAMPLE_PRODUCTS = Path(__file__).resolve().parents[1] / "sample_data" / "product_master.csv"
ITEM_TABLES = [
    "forecast_override_configs", "pos_transaction_items", "product_suppliers", "stock_master",
    "stock_master_location", "stock_movement", "stock_purchase_order_items", "stock_thresholds",
    "stock_transfer_order_items",
]


def _rows(table):
    return [r for f in sorted((CAPSTONE / table).iterdir()) for r in csv.DictReader(f.open(newline=""))]


def test_capstone_product_master_matches_storefront_catalog():
    capstone = {(r["item_number"], r["omni_item_id"], r["is_active"]) for r in _rows("product_master")}
    with SAMPLE_PRODUCTS.open(newline="") as f:
        storefront = {(r["item_number"], r["omni_item_id"], r["is_active"]) for r in csv.DictReader(f)}
    assert capstone == storefront


@pytest.mark.parametrize("table", ITEM_TABLES)
def test_capstone_item_references_exist_in_product_master(table):
    products = {(r["item_number"], r["omni_item_id"]) for r in _rows("product_master")}
    refs = {(r["item_number"], r["omni_item_id"]) for r in _rows(table)}
    assert refs and refs <= products


def test_capstone_suppliers_are_known():
    suppliers = {r["supplier_id"] for r in _rows("supplier_master")}
    assert {r["supplier_id"] for r in _rows("product_suppliers")} <= suppliers
    assert {r["supplier_id"] for r in _rows("stock_purchase_orders")} <= suppliers
