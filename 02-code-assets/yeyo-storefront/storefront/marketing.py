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

from typing import Dict, List, NamedTuple, Optional, Sequence, Tuple

from .config import Config

ACTION_SCALE = "Scale: raise bids while CPCs are low"
ACTION_STORE = 'Geo-target "collect in store today" at '
ACTION_SUBSTITUTE = "Swap creative to the own-brand substitute"
METRICS = ("sessions", "page_views", "cart_units", "purchase_sessions", "purchase_units", "revenue",
           "waitlist_units", "revenue_at_risk")


PHASE_BASELINE, PHASE_VIRAL, PHASE_SURGE, PHASE_SOLD_OUT = "baseline", "viral", "surge", "sold_out"
PHASE_STORES_OUT = "stores_out"
NEW_BID = "NEW: high bid"


def story_phase(started: bool, pause_done: bool, projected_units: int,
                store_units_left: Optional[int] = None) -> str:
    """Where the viral story is: before the spike, rising, marketing has reacted, partner sold out, stores sold out."""
    if not started:
        return PHASE_BASELINE
    if projected_units <= 0:
        return PHASE_STORES_OUT if store_units_left is not None and store_units_left <= 0 else PHASE_SOLD_OUT
    return PHASE_SURGE if pause_done else PHASE_VIRAL


def _bid(campaign, product, channel, targeting, cpc, budget, status) -> Dict:
    return {"Campaign": campaign, "Product": product, "Channel": channel, "Targeting": targeting,
            "Max CPC": f"${cpc:,.2f}", "Daily budget": f"${budget:,.0f}", "Status": status}


def campaign_bids(phase: str, hero: str, others: Sequence[Tuple[str, str]], substitute: Optional[str] = None,
                  stores: Sequence[str] = ()) -> List[Dict]:
    """Simulated ad bids (Google Ads is not connected) for each phase of the story.

    `others` are (product, search terms) pairs for the always-on campaigns."""
    rows = [_bid("Always-on", name, "Google Shopping", terms, 0.45 + 0.1 * i, 300, "Live")
            for i, (name, terms) in enumerate(others)]
    if phase not in (PHASE_SURGE, PHASE_SOLD_OUT, PHASE_STORES_OUT):
        return rows
    hero_status = NEW_BID if phase == PHASE_SURGE else "Paused: partner sold out"
    viral = [
        _bid("Viral: Shopping", hero, "Google Shopping", "tiktok eye tightener, instant eye firming", 2.40, 5000,
             hero_status),
        _bid("Viral: Search", hero, "Google Search", "viral under eye cream, temporary eye tightener", 1.80, 3000,
             hero_status),
        _bid("Viral: Social", hero, "TikTok + Instagram", "viewers of the viral video and lookalikes", 1.20, 4000,
             hero_status),
    ]
    if phase in (PHASE_SOLD_OUT, PHASE_STORES_OUT):
        viral.insert(0, _bid("Collect in store today", hero, "Local inventory ads",
                             "within 10 miles of " + (", ".join(stores) or "YeYo stores"), 2.20, 4000,
                             NEW_BID if phase == PHASE_SOLD_OUT else "Paused: stores sold out"))
        if substitute:
            viral.insert(1, _bid("Private label", substitute, "Google Shopping + TikTok + Instagram",
                                 "tiktok eye tightener, instant eye firming, viewers of the viral video", 1.60, 5000,
                                 NEW_BID))
    return viral + rows


class Banner(NamedTuple):
    campaign: str
    product: str
    badge: str
    headline: str
    detail: str
    cta: str
    tone: str


def campaign_banners(rows: Sequence[Dict], units_left: Optional[int] = None, limit: int = 4) -> List[Banner]:
    """Ad creatives for the live campaigns in `campaign_bids` rows (paused ones are left out).

    The three viral campaigns on the same product share one banner."""
    banners: List[Banner] = []
    viral_channels: List[str] = []
    for r in rows:
        if r["Status"].startswith("Paused"):
            continue
        campaign, product = r["Campaign"], r["Product"]
        if campaign.startswith("Viral"):
            viral_channels.append(r["Channel"])
            if len(viral_channels) > 1:
                continue
            hurry = units_left is not None and units_left > 0
            banners.append(Banner(campaign, product,
                                  f"HURRY: ONLY {units_left:,} LEFT" if hurry else "AS SEEN ON TIKTOK",
                                  "Trending on TikTok", "", "BUY BEFORE IT'S GONE", "hurry"))
        elif campaign == "Collect in store today":
            banners.append(Banner(campaign, product, "CLICK & COLLECT TODAY", "Collect it in store today",
                                  r["Targeting"].replace("within 10 miles of ", "At "), "COLLECT IN STORE", "store"))
        elif campaign == "Private label":
            banners.append(Banner(campaign, product, "NEW: YEYO OWN BRAND", "Try our own-brand alternative", "",
                                  "SHOP THE ALTERNATIVE", "brand"))
        else:
            banners.append(Banner(campaign, product, "EVERYDAY ESSENTIALS", r["Targeting"].capitalize(), "",
                                  "SHOP NOW", "always"))
    if len(viral_channels) > 1:
        i = next(i for i, b in enumerate(banners) if b.tone == "hurry")
        banners[i] = banners[i]._replace(detail=" · ".join(viral_channels))
    return banners[:limit]


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
