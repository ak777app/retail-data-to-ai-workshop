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

"""Marketing tab: traffic source, funnel, revenue at risk and next best action for the viral moment."""

from typing import Dict, List, Optional

from .config import Config

ACTION_SCALE = "Scale: raise bids while CPCs are low"
ACTION_STORE = 'Geo-target "collect in store today" at '
ACTION_SUBSTITUTE = "Swap creative to the own-brand substitute"
METRICS = ("sessions", "page_views", "cart_units", "purchase_sessions", "purchase_units", "revenue",
           "waitlist_units", "revenue_at_risk")


def next_best_action(risk_level: Optional[str], nearest_store: Optional[str]) -> str:
    """Same rule as vw_marketing_viral.next_best_action."""
    if (risk_level or "OK") == "OK":
        return ACTION_SCALE
    if nearest_store:
        return ACTION_STORE + nearest_store
    return ACTION_SUBSTITUTE


def _sums() -> str:
    return ",\n       ".join(f"SUM({m}) AS {m}" for m in METRICS)


def by_referrer_sql(config: Config) -> str:
    return f"""
SELECT referrer,
       {_sums()},
       SAFE_DIVIDE(SUM(purchase_sessions), SUM(sessions)) AS conversion
FROM `{config.fq_table(config.marketing_view)}`
WHERE viral_id = @viral_id
GROUP BY referrer ORDER BY sessions DESC"""


def by_postcode_sql(config: Config) -> str:
    return f"""
SELECT postal_code,
       {_sums()},
       SAFE_DIVIDE(SUM(purchase_sessions), SUM(sessions)) AS conversion,
       ANY_VALUE(nearest_store) AS nearest_store,
       ANY_VALUE(nearest_store_units) AS nearest_store_units,
       ANY_VALUE(next_best_action) AS next_best_action
FROM `{config.fq_table(config.marketing_view)}`
WHERE viral_id = @viral_id
GROUP BY postal_code ORDER BY sessions DESC"""


def _row(counts: Dict[str, int], price: float) -> Dict:
    row = {
        "sessions": counts.get("page_view", 0),
        "page_views": counts.get("page_view", 0),
        "cart_units": counts.get("add_to_cart_units", 0),
        "purchase_sessions": counts.get("purchase", 0),
        "purchase_units": counts.get("purchase_units", 0),
        "waitlist_units": counts.get("waitlist_join_units", 0),
    }
    row["revenue"] = round(row["purchase_units"] * price, 2)
    row["revenue_at_risk"] = round(row["waitlist_units"] * price, 2)
    row["conversion"] = row["purchase_sessions"] / row["sessions"] if row["sessions"] else 0.0
    return row


def simulator_rows(snapshot: Dict, price: float, risk_level: str) -> Dict[str, List[Dict]]:
    """Fallback when BigQuery is not available (local mode): same columns, from the simulator counters."""
    referrers = [{"referrer": k, **_row(c, price)} for k, c in snapshot["by_referrer"].items()]
    postcodes = [{"postal_code": k, **_row(c, price), "nearest_store": None, "nearest_store_units": None,
                  "next_best_action": next_best_action(risk_level, None)} for k, c in snapshot["by_postal"].items()]
    return {"referrer": sorted(referrers, key=lambda r: -r["sessions"]),
            "postcode": sorted(postcodes, key=lambda r: -r["sessions"])}


def totals(rows: List[Dict]) -> Dict:
    out = {m: sum(float(r.get(m) or 0) for r in rows) for m in METRICS}
    out["conversion"] = out["purchase_sessions"] / out["sessions"] if out["sessions"] else 0.0
    social = sum(float(r["sessions"] or 0) for r in rows if "tiktok" in (r.get("referrer") or ""))
    out["tiktok_share"] = social / out["sessions"] if out["sessions"] else 0.0
    return out
