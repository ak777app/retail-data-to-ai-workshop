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

import json
import time
from concurrent.futures import Future

from storefront.events import CartLine, build_order_event
from storefront.publisher import AutoPublisher, BigQueryPublisher, LocalFilePublisher, encode_event
from storefront.spike import STATUS_CANCELLED, STATUS_DONE, SpikeConfig, SpikeJob


class FakePubSubClient:
    def __init__(self, fail=False):
        self.fail = fail
        self.messages = []

    def publish(self, topic, data, **attrs):
        f = Future()
        if self.fail:
            f.set_exception(RuntimeError("404 Topic not found"))
        else:
            self.messages.append((topic, json.loads(data), attrs))
            f.set_result(str(len(self.messages)))
        return f


class FakeBigQueryClient:
    def __init__(self):
        self.rows = {}

    def insert_rows_json(self, table, rows, row_ids=None):
        self.rows.setdefault(table, []).extend(rows)
        return []


def _event(catalog, channel="online"):
    p = catalog.products_for_channel(channel)[0]
    listing = catalog.listings_for(p.omni_item_id)[0] if channel == "marketplace" else None
    return build_order_event(catalog, [CartLine(p, 1, listing)], channel=channel)


def test_encode_event_roundtrip(catalog):
    ev = _event(catalog)
    assert json.loads(encode_event(ev)) == ev


def test_bigquery_publisher_routes_by_channel(catalog, config):
    bq = FakeBigQueryClient()
    pub = BigQueryPublisher(config, client=bq)
    futures = pub.publish_many([_event(catalog, "online"), _event(catalog, "marketplace")])
    assert all(f.exception() is None for f in futures)
    assert set(bq.rows) == {"test-project.capstone_ds.online_orders", "test-project.capstone_ds.marketplace_orders"}
    row = bq.rows["test-project.capstone_ds.online_orders"][0]
    assert row["subscription_name"] == "bigquery-direct-fallback" and row["message_id"] == row["event_id"]


def test_auto_publisher_uses_pubsub_with_channel_attribute(catalog, config):
    from storefront.publisher import PubSubPublisher

    client = FakePubSubClient()
    pub = AutoPublisher(config, primary_factory=lambda: PubSubPublisher(config, client=client))
    assert pub.publish(_event(catalog, "marketplace")).result() == "1"
    topic, data, attrs = client.messages[0]
    assert topic == "projects/test-project/topics/yeyo-orders" and attrs["channel"] == "marketplace"
    assert not pub.using_fallback


def test_auto_publisher_falls_back_to_bigquery(catalog, config):
    from storefront.publisher import PubSubPublisher

    bq = FakeBigQueryClient()
    pub = AutoPublisher(
        config,
        primary_factory=lambda: PubSubPublisher(config, client=FakePubSubClient(fail=True)),
        fallback_factory=lambda: BigQueryPublisher(config, client=bq),
    )
    pub.publish(_event(catalog)).result(timeout=5)
    assert pub.using_fallback and "Topic not found" in pub.fallback_reason
    pub.publish(_event(catalog)).result(timeout=5)
    assert len(bq.rows["test-project.capstone_ds.online_orders"]) == 2


def test_auto_publisher_falls_back_when_client_cannot_be_created(catalog, config):
    bq = FakeBigQueryClient()

    def broken():
        raise RuntimeError("no credentials")

    pub = AutoPublisher(config, primary_factory=broken, fallback_factory=lambda: BigQueryPublisher(config, client=bq))
    pub.publish(_event(catalog)).result(timeout=5)
    assert pub.using_fallback


def test_spike_publishes_all_events_through_shared_publisher(catalog, config):
    pub = LocalFilePublisher(config)
    job = SpikeJob(catalog, pub, SpikeConfig(num_orders=150, duration_seconds=0, seed=1)).start()
    job.join(timeout=30)
    snap = job.snapshot()
    assert snap["status"] == STATUS_DONE and snap["published"] == 150 and snap["failed"] == 0
    assert set(snap["by_channel"]) <= {"online", "marketplace"}
    with open(config.local_events_file) as f:
        events = [json.loads(line) for line in f]
    assert len(events) == 150 and {e["spike_id"] for e in events} == {job.spike_id}


def test_spike_spreads_events_over_window_and_is_cancellable(catalog, config):
    client = FakePubSubClient()
    from storefront.publisher import PubSubPublisher

    job = SpikeJob(catalog, PubSubPublisher(config, client=client),
                   SpikeConfig(num_orders=500, duration_seconds=30, seed=3)).start()
    time.sleep(1.0)
    mid = job.snapshot()
    assert job.is_running and 0 < mid["published"] < 500
    job.cancel()
    job.join(timeout=5)
    snap = job.snapshot()
    assert snap["status"] == STATUS_CANCELLED and snap["published"] == len(client.messages) < 500
    ts = [m[1]["order_ts"] for m in client.messages]
    assert ts == sorted(ts)


def test_spike_counts_failures(catalog, config):
    from storefront.publisher import PubSubPublisher

    job = SpikeJob(catalog, PubSubPublisher(config, client=FakePubSubClient(fail=True)),
                   SpikeConfig(num_orders=10, duration_seconds=0)).start()
    job.join(timeout=10)
    snap = job.snapshot()
    assert snap["failed"] == 10 and snap["published"] == 0 and snap["errors"]


def test_spike_config_validation():
    import pytest

    with pytest.raises(ValueError):
        SpikeConfig(num_orders=5000).validate(2000, 600)
    with pytest.raises(ValueError):
        SpikeConfig(duration_seconds=900).validate(2000, 600)
