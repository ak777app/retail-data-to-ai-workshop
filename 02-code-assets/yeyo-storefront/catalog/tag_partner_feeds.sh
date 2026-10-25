#!/usr/bin/env bash
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Tags every partner stock asset (GCP and AWS) in Dataplex Universal Catalog with the
# yeyo-partner-feed aspect, so one catalog search lists both clouds side by side.
# Usage: bash catalog/tag_partner_feeds.sh YOUR_PROJECT_ID
set -euo pipefail

PROJECT_ID="${1:?usage: tag_partner_feeds.sh YOUR_PROJECT_ID}"
LOCATION="${LOCATION:-us-central1}"
AWS_LOCATION="${AWS_LOCATION:-aws-us-east-1}"
AWS_DATASET="${AWS_DATASET:-yeyo_partner_aws}"
ASPECT="yeyo-partner-feed"
DIR="$(cd "$(dirname "$0")" && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

if ! gcloud dataplex aspect-types describe "$ASPECT" --project="$PROJECT_ID" --location=global >/dev/null 2>&1; then
  gcloud dataplex aspect-types create "$ASPECT" --project="$PROJECT_ID" --location=global \
    --display-name="YeYo partner feed" \
    --description="Partner stock assets across Google Cloud and AWS" \
    --metadata-template-file-name="$DIR/partner_feed_aspect_type.yaml" >/dev/null
fi

# Omni (aws-us-east-1) entries live in the catalog of the paired Google Cloud region.
case "$AWS_LOCATION" in
  aws-us-east-1) AWS_CATALOG_LOCATION=us-east4 ;;
  *) AWS_CATALOG_LOCATION="${AWS_CATALOG_LOCATION:?set AWS_CATALOG_LOCATION for $AWS_LOCATION}" ;;
esac

# tag LOCATION ENTRY_GROUP ENTRY PARTNER SELLER CLOUD STORAGE FORMAT WRITTEN_BY REFRESH
tag() {
  local file="$TMP/aspect.yaml"
  cat >"$file" <<YAML
$PROJECT_ID.global.$ASPECT:
  data:
    partner: "$4"
    seller_id: "$5"
    cloud: "$6"
    storage: "$7"
    table_format: "$8"
    written_by: "$9"
    refresh: "${10}"
YAML
  # A new aspect type can take a minute before entries accept it, so retry.
  for attempt in 1 2 3 4 5 6; do
    if gcloud dataplex entries update "$3" --project="$PROJECT_ID" --location="$1" \
        --entry-group="$2" --update-aspects="$file" >/dev/null 2>"$TMP/err"; then
      echo "tagged ${3##*/}"
      TAGGED=$((TAGGED + 1))
      return
    fi
    sleep 15
  done
  echo "skipped ${3##*/}: $(tail -1 "$TMP/err")" >&2
}

TAGGED=0
BQ="bigquery.googleapis.com/projects/$PROJECT_ID/datasets"
tag "$LOCATION" @biglake \
  "biglake.googleapis.com/projects/$PROJECT_ID/catalogs/yeyo_lake/namespaces/yeyo_partner_lake/tables/partner_stock" \
  "Glow Lab" MKT-S004 GCP "Cloud Storage" Iceberg "Serverless Spark" "Every Reset demo"
tag "$AWS_CATALOG_LOCATION" "@bigquery-$AWS_LOCATION" "$BQ/$AWS_DATASET/tables/fitgear_stock" \
  "FitGear Direct" MKT-S003 AWS "Amazon S3" Iceberg "Amazon Athena" "One-off"
tag "$AWS_CATALOG_LOCATION" @bigquery "$BQ/yeyo_partner_east4/tables/fitgear_low_stock" \
  "FitGear Direct" MKT-S003 GCP BigQuery "BigQuery table" "BigQuery Omni query result" "Each Omni run"
tag "$LOCATION" @bigquery "$BQ/capstone_ds/tables/vw_partner_stock" \
  "Glow Lab" MKT-S004 GCP BigQuery "BigQuery view" "View over the Iceberg table" "Live"

if [ "$TAGGED" -eq 0 ]; then
  echo "No partner stock assets were tagged; check the project ID and that the tables exist." >&2
  exit 1
fi
