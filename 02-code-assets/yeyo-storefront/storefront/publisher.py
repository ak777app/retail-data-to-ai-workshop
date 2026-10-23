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

"""Publishes order events to Pub/Sub, with a BigQuery-direct fallback.

Every publisher returns a `concurrent.futures.Future` per event so single
orders and the spike simulator share the same code path.

Modes (PUBLISH_MODE):
  pubsub   - publish to the Module-11 topic; BigQuery subscriptions land the rows.
  bigquery - stream rows straight into the Module-11 tables (no Pub/Sub).
  auto     - Pub/Sub first; on the first failure switch to bigquery.
  local    - append events to a JSONL file (offline rehearsal, no GCP).
"""

import json
import logging
import threading
from collections import defaultdict
from concurrent.futures import Future
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

from .config import Config
from .events import event_attributes, format_ts

logger = logging.getLogger(__name__)

FALLBACK_SUBSCRIPTION_NAME = "bigquery-direct-fallback"


def _done(result=None, exc: Optional[BaseException] = None) -> Future:
    f: Future = Future()
    if exc is not None:
        f.set_exception(exc)
    else:
        f.set_result(result)
    return f


def encode_event(event: Dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


class BasePublisher:
    mode = "base"

    def describe(self) -> str:
        return self.mode

    def publish(self, event: Dict) -> Future:
        raise NotImplementedError

    def publish_many(self, events: Iterable[Dict]) -> List[Future]:
        return [self.publish(e) for e in events]

    def flush(self) -> None:
        pass


class PubSubPublisher(BasePublisher):
    mode = "pubsub"

    def __init__(self, config: Config, client=None):
        if not config.project_id:
            raise ValueError("PROJECT_ID is required for Pub/Sub publishing")
        if client is None:
            from google.cloud import pubsub_v1

            client = pubsub_v1.PublisherClient(
                batch_settings=pubsub_v1.types.BatchSettings(
                    max_messages=100, max_bytes=1024 * 1024, max_latency=0.05,
                )
            )
        self._client = client
        self.topic_path = config.topic_path

    def describe(self) -> str:
        return f"Pub/Sub topic {self.topic_path}"

    def publish(self, event: Dict) -> Future:
        return self._client.publish(self.topic_path, encode_event(event), **event_attributes(event))


def event_to_row(event: Dict, publish_time: Optional[datetime] = None) -> Dict:
    """Row shaped like a Pub/Sub BigQuery-subscription write (incl. metadata columns)."""
    row = dict(event)
    row["subscription_name"] = FALLBACK_SUBSCRIPTION_NAME
    row["message_id"] = event["event_id"]
    row["publish_time"] = format_ts(publish_time or datetime.now(timezone.utc))
    row["attributes"] = json.dumps(event_attributes(event))
    return row


class BigQueryPublisher(BasePublisher):
    mode = "bigquery"

    def __init__(self, config: Config, client=None):
        if client is None:
            from google.cloud import bigquery

            client = bigquery.Client(project=config.project_id or None, location=config.location)
        self._client = client
        self._config = config

    def describe(self) -> str:
        return f"BigQuery direct ({self._config.project_id}.{self._config.bq_dataset})"

    def publish(self, event: Dict) -> Future:
        return self.publish_many([event])[0]

    def publish_many(self, events: Iterable[Dict]) -> List[Future]:
        events = list(events)
        futures: List[Optional[Future]] = [None] * len(events)
        by_table: Dict[str, List[int]] = defaultdict(list)
        for i, e in enumerate(events):
            by_table[self._config.orders_table_for_channel(e["channel"])].append(i)
        for table, idxs in by_table.items():
            rows = [event_to_row(events[i]) for i in idxs]
            try:
                errors = self._client.insert_rows_json(table, rows, row_ids=[r["event_id"] for r in rows])
            except Exception as exc:
                for i in idxs:
                    futures[i] = _done(exc=exc)
                continue
            failed = {err.get("index"): err.get("errors") for err in errors or []}
            for pos, i in enumerate(idxs):
                if pos in failed:
                    futures[i] = _done(exc=RuntimeError(f"BigQuery insert failed for {table}: {failed[pos]}"))
                else:
                    futures[i] = _done(events[i]["event_id"])
        return futures  # type: ignore[return-value]


class LocalFilePublisher(BasePublisher):
    mode = "local"

    def __init__(self, config: Config):
        self.path = config.local_events_file
        self._lock = threading.Lock()

    def describe(self) -> str:
        return f"local file {self.path}"

    def publish(self, event: Dict) -> Future:
        with self._lock, open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")
        return _done(event["event_id"])


class AutoPublisher(BasePublisher):
    """Pub/Sub first; permanently switches to BigQuery-direct after a failure."""

    mode = "auto"

    def __init__(self, config: Config, primary_factory=None, fallback_factory=None):
        self._config = config
        self._fallback_factory = fallback_factory or (lambda: BigQueryPublisher(config))
        self._lock = threading.Lock()
        self._fallback: Optional[BasePublisher] = None
        self.fallback_reason: Optional[str] = None
        try:
            self._primary: Optional[BasePublisher] = (primary_factory or (lambda: PubSubPublisher(config)))()
        except Exception as exc:
            self._primary = None
            self._switch(exc)

    @property
    def active(self) -> BasePublisher:
        return self._fallback or self._primary  # type: ignore[return-value]

    @property
    def using_fallback(self) -> bool:
        return self._fallback is not None

    def describe(self) -> str:
        if self.using_fallback:
            return f"{self.active.describe()} (fallback: {self.fallback_reason})"
        return self.active.describe()

    def _switch(self, exc: BaseException) -> BasePublisher:
        with self._lock:
            if self._fallback is None:
                logger.warning("Pub/Sub unavailable, falling back to BigQuery direct: %s", exc)
                self.fallback_reason = f"{exc.__class__.__name__}: {exc}"[:300]
                self._fallback = self._fallback_factory()
            return self._fallback

    def publish(self, event: Dict) -> Future:
        return self.publish_many([event])[0]

    def publish_many(self, events: Iterable[Dict]) -> List[Future]:
        events = list(events)
        if self._fallback is not None:
            return self._fallback.publish_many(events)
        try:
            primary_futures = self._primary.publish_many(events)  # type: ignore[union-attr]
        except Exception as exc:
            return self._switch(exc).publish_many(events)
        return [self._chain(f, e) for f, e in zip(primary_futures, events)]

    def _chain(self, primary: Future, event: Dict) -> Future:
        out: Future = Future()

        def _on_done(f: Future) -> None:
            exc = f.exception()
            if exc is None:
                out.set_result(f.result())
                return
            try:
                retry = self._switch(exc).publish(event)
                retry.add_done_callback(
                    lambda r: out.set_exception(r.exception()) if r.exception() else out.set_result(r.result())
                )
            except Exception as fallback_exc:
                out.set_exception(fallback_exc)

        primary.add_done_callback(_on_done)
        return out

    def flush(self) -> None:
        self.active.flush()


def create_publisher(config: Config) -> BasePublisher:
    if config.publish_mode == "pubsub":
        return PubSubPublisher(config)
    if config.publish_mode == "bigquery":
        return BigQueryPublisher(config)
    if config.publish_mode == "local":
        return LocalFilePublisher(config)
    return AutoPublisher(config)
