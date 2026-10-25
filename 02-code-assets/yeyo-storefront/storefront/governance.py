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

"""Sets up the governed catalog for the Data platform tab (Module 13 section 10).

  python -m storefront.governance YOUR_PROJECT_ID --presenter you@example.com

Creates the yeyo-governed-asset aspect type and tags orders, clickstream, partner stock (GCP and AWS),
alerts and customers with it, then protects customer email and phone with policy tags and masking rules:
the storefront and data agent service accounts read masked values, the presenter reads the real ones.
"""

import argparse
import sys
import time
from typing import Dict, List

from .config import load_config
from .ops import _authorized_session
from .platform import DATAPLEX_API, GOVERNED_ASPECT, governed_assets

DATACATALOG_API = "https://datacatalog.googleapis.com/v1"
DATAPOLICY_API = "https://bigquerydatapolicy.googleapis.com/v1"
TAXONOMY = "YeYo governance"
FIELDS = ("domain", "cloud", "storage", "sensitivity", "access_control", "owner")
# column -> (policy tag, data policy id, masking rule)
PII_COLUMNS = {
    "email": ("Customer email", "yeyo_customer_email_mask", "EMAIL_MASK"),
    "phone_number": ("Customer phone", "yeyo_customer_phone_mask", "LAST_FOUR_CHARACTERS"),
}


def _ok(resp, allow=(409,)):
    if resp.status_code not in allow:
        resp.raise_for_status()
    return resp


def ensure_aspect_type(s, project: str) -> None:
    url = f"{DATAPLEX_API}/projects/{project}/locations/global/aspectTypes"
    if s.get(f"{url}/{GOVERNED_ASPECT}", timeout=30).status_code == 200:
        return
    fields = [{"name": f, "type": "string", "index": i + 1,
               "annotations": {"displayName": f.replace("_", " ").capitalize()}} for i, f in enumerate(FIELDS)]
    _ok(s.post(url, params={"aspectTypeId": GOVERNED_ASPECT}, timeout=30, json={
        "displayName": "YeYo governed asset",
        "description": "Every viral-moment asset (orders, clickstream, partner stock on GCP and AWS, alerts)",
        "metadataTemplate": {"name": GOVERNED_ASPECT, "type": "record", "recordFields": fields}}))


def tag_assets(s, config) -> int:
    key = f"{config.project_id}.global.{GOVERNED_ASPECT}"
    tagged = 0
    for a in governed_assets(config):
        url = (f"{DATAPLEX_API}/projects/{config.project_id}/locations/{a.catalog_location}/entryGroups/"
               f"{a.entry_group}/entries/{a.entry}")
        body = {"aspects": {key: {"data": {f: getattr(a, f) for f in FIELDS}}}}
        for attempt in range(6):  # a new aspect type takes a minute before entries accept it
            resp = s.patch(url, params={"updateMask": "aspects", "aspectKeys": key}, json=body, timeout=30)
            if resp.ok:
                print(f"tagged {a.name}")
                tagged += 1
                break
            if resp.status_code == 404:
                print(f"skipped {a.name}: not found", file=sys.stderr)
                break
            time.sleep(15)
        else:
            print(f"skipped {a.name}: {resp.status_code} {resp.text[:200]}", file=sys.stderr)
    return tagged


def ensure_policy_tags(s, project: str, location: str) -> Dict[str, str]:
    url = f"{DATACATALOG_API}/projects/{project}/locations/{location}/taxonomies"
    taxonomy = next((t["name"] for t in s.get(url, timeout=30).json().get("taxonomies", [])
                     if t["displayName"] == TAXONOMY), None)
    if not taxonomy:
        taxonomy = _ok(s.post(url, timeout=30, json={
            "displayName": TAXONOMY, "activatedPolicyTypes": ["FINE_GRAINED_ACCESS_CONTROL"],
            "description": "Customer PII in capstone_ds"})).json()["name"]
    existing = {t["displayName"]: t["name"]
                for t in s.get(f"{DATACATALOG_API}/{taxonomy}/policyTags", timeout=30).json().get("policyTags", [])}
    tags = {}
    for column, (display, _, _) in PII_COLUMNS.items():
        tags[column] = existing.get(display) or _ok(s.post(
            f"{DATACATALOG_API}/{taxonomy}/policyTags", json={"displayName": display}, timeout=30)).json()["name"]
    return {"taxonomy": taxonomy, **tags}


def grant(s, resource_url: str, role: str, members: List[str]) -> None:
    policy = _ok(s.post(f"{resource_url}:getIamPolicy", json={}, timeout=30), allow=()).json()
    bindings = [b for b in policy.get("bindings", []) if b["role"] != role]
    bindings.append({"role": role, "members": sorted(set(members))})
    _ok(s.post(f"{resource_url}:setIamPolicy", json={"policy": {**policy, "bindings": bindings}}, timeout=30),
        allow=())


def ensure_masking(s, project: str, location: str, tags: Dict[str, str], masked_readers: List[str]) -> None:
    url = f"{DATAPOLICY_API}/projects/{project}/locations/{location}/dataPolicies"
    for column, (_, policy_id, rule) in PII_COLUMNS.items():
        _ok(s.post(url, timeout=30, json={
            "dataPolicyId": policy_id, "dataPolicyType": "DATA_MASKING_POLICY", "policyTag": tags[column],
            "dataMaskingPolicy": {"predefinedExpression": rule}}))
        grant(s, f"{url}/{policy_id}", "roles/bigquerydatapolicy.maskedReader", masked_readers)


def apply_policy_tags(config, tags: Dict[str, str]) -> None:
    from google.cloud import bigquery

    client = bigquery.Client(project=config.project_id, location=config.location)
    table = client.get_table(config.fq_table("customer_master"))
    table.schema = [bigquery.SchemaField(f.name, f.field_type, mode=f.mode, description=f.description,
                                         policy_tags=bigquery.PolicyTagList([tags[f.name]]))
                    if f.name in PII_COLUMNS else f for f in table.schema]
    client.update_table(table, ["schema"])


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("project_id")
    parser.add_argument("--presenter", required=True, help="Google account that should see unmasked PII")
    parser.add_argument("--masked-reader", action="append", default=[],
                        help="service account that reads masked PII (default: the storefront and data agent)")
    args = parser.parse_args(argv)
    config = load_config()
    config = type(config)(**{**config.__dict__, "project_id": args.project_id})
    masked = args.masked_reader or [f"yeyo-{name}-sa@{args.project_id}.iam.gserviceaccount.com"
                                    for name in ("storefront", "data-agent")]
    s = _authorized_session()

    ensure_aspect_type(s, args.project_id)
    if tag_assets(s, config) == 0:
        print("No assets were tagged; check the project ID and that the tables exist.", file=sys.stderr)
        return 1
    tags = ensure_policy_tags(s, args.project_id, config.location)
    ensure_masking(s, args.project_id, config.location, tags, [f"serviceAccount:{sa}" for sa in masked])
    grant(s, f"{DATACATALOG_API}/{tags['taxonomy']}", "roles/datacatalog.categoryFineGrainedReader",
          [f"user:{args.presenter}"])
    apply_policy_tags(config, tags)
    print(f"customer_master: {', '.join(PII_COLUMNS)} protected by policy tags in '{TAXONOMY}'")
    return 0


if __name__ == "__main__":
    sys.exit(main())
