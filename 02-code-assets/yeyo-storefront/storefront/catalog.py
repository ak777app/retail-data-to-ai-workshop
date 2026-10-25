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

"""Product catalog, marketplace sellers/listings, customers and locations.

Reads the reskinned `product_master` (and marketplace seller/listing tables) from
BigQuery, or the bundled CSVs in `sample_data/` so the storefront works before
BigQuery is loaded.
"""

import csv
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, List

from .config import CHANNELS, SAMPLE_DATA_DIR, Config

logger = logging.getLogger(__name__)

DEFAULT_ONLINE_FULFILLMENT_LOCATION = "WHE-IL-WH"
PRODUCT_IMAGE_DIR = SAMPLE_DATA_DIR.parent / "static" / "products"


@dataclass(frozen=True)
class Product:
    item_number: str
    omni_item_id: str
    description: str
    category: str
    sub_category: str
    brand: str
    price: Decimal
    image_uri: str = ""
    is_active: bool = True
    channels: tuple = CHANNELS

    def sold_on(self, channel: str) -> bool:
        return channel in self.channels


@dataclass(frozen=True)
class Seller:
    seller_id: str
    seller_name: str
    fulfillment_type: str
    rating: float
    is_active: bool = True


@dataclass(frozen=True)
class Listing:
    listing_id: str
    seller_id: str
    item_number: str
    omni_item_id: str
    listing_price: Decimal
    stock_status: str = "IN_STOCK"
    is_active: bool = True


@dataclass(frozen=True)
class Location:
    location_id: str
    location_name: str
    location_type: str


@dataclass
class Catalog:
    products: List[Product]
    sellers: Dict[str, Seller]
    listings: List[Listing]
    customer_ids: List[str]
    locations: List[Location]
    source: str = "sample"
    warnings: List[str] = field(default_factory=list)

    def product(self, omni_item_id: str) -> Product:
        for p in self.products:
            if p.omni_item_id == omni_item_id:
                return p
        raise KeyError(omni_item_id)

    def products_for_channel(self, channel: str) -> List[Product]:
        products = [p for p in self.products if p.sold_on(channel)]
        if channel == "marketplace":
            listed = {l.omni_item_id for l in self.active_listings()}
            products = [p for p in products if p.omni_item_id in listed]
        return products

    def active_listings(self) -> List[Listing]:
        return [
            l for l in self.listings
            if l.is_active and l.seller_id in self.sellers and self.sellers[l.seller_id].is_active
        ]

    def listings_for(self, omni_item_id: str) -> List[Listing]:
        return [l for l in self.active_listings() if l.omni_item_id == omni_item_id]

    def stores(self) -> List[Location]:
        return [l for l in self.locations if l.location_type.lower() == "store"]

    def categories(self) -> List[str]:
        return sorted({p.category for p in self.products})


def _flag(value) -> bool:
    return str(value if value is not None else "Y").strip().upper() in ("Y", "YES", "TRUE", "1")


def _channels(value) -> tuple:
    if not value:
        return CHANNELS
    parsed = tuple(c.strip().lower() for c in str(value).replace(",", "|").split("|") if c.strip())
    return tuple(c for c in parsed if c in CHANNELS) or CHANNELS


def _first(row: dict, *keys, default=""):
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return v
    return default


def product_image(product: Product) -> str:
    """An http(s) image_uri, else the bundled packshot static/products/<item_number>.jpg, else ""."""
    if product.image_uri.startswith(("http://", "https://")):
        return product.image_uri
    path = PRODUCT_IMAGE_DIR / f"{product.item_number}.jpg"
    return str(path) if path.exists() else ""


def product_from_row(row: dict) -> Product:
    """Maps a product_master row (reskinned or original column names) to a Product."""
    item_number = str(_first(row, "item_number"))
    return Product(
        item_number=item_number,
        omni_item_id=str(_first(row, "omni_item_id", default=item_number)),
        description=str(_first(row, "description", "item_name", default=item_number)),
        category=str(_first(row, "category", "product_category", "appliance_type", default="general")),
        sub_category=str(_first(row, "sub_category", "product_sub_category", "appliance_sub_type")),
        brand=str(_first(row, "brand")),
        price=Decimal(str(_first(row, "price", default="0"))),
        image_uri=str(_first(row, "product_image_gcs_uri", "image_uri")),
        is_active=_flag(row.get("is_active")),
        channels=_channels(row.get("channels")),
    )


def seller_from_row(row: dict) -> Seller:
    return Seller(
        seller_id=str(row["seller_id"]),
        seller_name=str(_first(row, "seller_name", default=row["seller_id"])),
        fulfillment_type=str(_first(row, "fulfillment_type", default="SELLER_FULFILLED")).upper(),
        rating=float(_first(row, "rating", default=0) or 0),
        is_active=_flag(row.get("is_active")),
    )


def listing_from_row(row: dict) -> Listing:
    return Listing(
        listing_id=str(row["listing_id"]),
        seller_id=str(row["seller_id"]),
        item_number=str(_first(row, "item_number")),
        omni_item_id=str(row["omni_item_id"]),
        listing_price=Decimal(str(_first(row, "listing_price", "price", default="0"))),
        stock_status=str(_first(row, "stock_status", default="IN_STOCK")),
        is_active=_flag(row.get("is_active")),
    )


def _read_csv(path: Path) -> List[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def synthesize_listings(products: Iterable[Product], sellers: Dict[str, Seller]) -> List[Listing]:
    """Deterministic marketplace listings for catalogs that have no listing table yet."""
    active = sorted(s.seller_id for s in sellers.values() if s.is_active)
    listings: List[Listing] = []
    if not active:
        return listings
    for i, p in enumerate(p for p in products if p.sold_on("marketplace")):
        seller_id = active[i % len(active)]
        listings.append(Listing(
            listing_id=f"LST-{p.omni_item_id}-{seller_id}",
            seller_id=seller_id,
            item_number=p.item_number,
            omni_item_id=p.omni_item_id,
            listing_price=(p.price * Decimal("0.95")).quantize(Decimal("0.01")),
        ))
    return listings


def load_sample_catalog(data_dir: Path = SAMPLE_DATA_DIR) -> Catalog:
    products = [product_from_row(r) for r in _read_csv(data_dir / "product_master.csv")]
    sellers = {s.seller_id: s for s in (seller_from_row(r) for r in _read_csv(data_dir / "marketplace_sellers.csv"))}
    listings = [listing_from_row(r) for r in _read_csv(data_dir / "marketplace_listings.csv")]
    customers = [r["customer_id"] for r in _read_csv(data_dir / "customers.csv")]
    locations = [Location(r["location_id"], r["location_name"], r["location_type"])
                 for r in _read_csv(data_dir / "locations.csv")]
    return Catalog(
        products=[p for p in products if p.is_active],
        sellers=sellers,
        listings=listings,
        customer_ids=customers,
        locations=locations,
        source="sample",
    )


def _query(client, sql: str) -> List[dict]:
    return [dict(row.items()) for row in client.query(sql).result()]


def load_bigquery_catalog(config: Config, client=None) -> Catalog:
    from google.cloud import bigquery

    client = client or bigquery.Client(project=config.project_id or None, location=config.location)
    sample = load_sample_catalog()
    warnings: List[str] = []

    rows = _query(client, f"SELECT * FROM `{config.fq_table(config.product_master_table)}`")
    products = [p for p in (product_from_row(r) for r in rows) if p.is_active]
    if not products:
        raise RuntimeError(f"No active products in {config.fq_table(config.product_master_table)}")

    try:
        seller_rows = _query(client, f"SELECT * FROM `{config.fq_table(config.marketplace_sellers_table)}`")
        sellers = {s.seller_id: s for s in (seller_from_row(r) for r in seller_rows)}
    except Exception as exc:  # table not created yet
        warnings.append(f"Marketplace sellers table unavailable ({exc.__class__.__name__}); using bundled sellers.")
        sellers = sample.sellers

    try:
        listing_rows = _query(client, f"SELECT * FROM `{config.fq_table(config.marketplace_listings_table)}`")
        listings = [listing_from_row(r) for r in listing_rows]
    except Exception as exc:
        warnings.append(f"Marketplace listings table unavailable ({exc.__class__.__name__}); synthesizing listings.")
        listings = []

    known = {p.omni_item_id for p in products}
    if not any(l.omni_item_id in known for l in listings):
        bundled = [l for l in sample.listings if l.omni_item_id in known and l.seller_id in sellers]
        covered = {l.omni_item_id for l in bundled}
        listings = bundled + synthesize_listings([p for p in products if p.omni_item_id not in covered], sellers)

    return Catalog(
        products=products,
        sellers=sellers,
        listings=listings,
        customer_ids=sample.customer_ids,
        locations=sample.locations,
        source="bigquery",
        warnings=warnings,
    )


def load_catalog(config: Config, client=None) -> Catalog:
    if config.catalog_source == "sample":
        return load_sample_catalog()
    try:
        return load_bigquery_catalog(config, client=client)
    except Exception as exc:
        if config.catalog_source == "bigquery":
            raise
        logger.warning("BigQuery catalog unavailable, using bundled sample: %s", exc)
        catalog = load_sample_catalog()
        catalog.warnings.append(f"BigQuery catalog unavailable ({exc.__class__.__name__}); using bundled sample data.")
        return catalog
