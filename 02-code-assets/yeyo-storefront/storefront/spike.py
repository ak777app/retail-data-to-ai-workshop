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

"""Background order-spike simulator.

Generates N synthetic online/marketplace orders with timestamps spread across a
burst window and publishes each through the same publisher used for single
orders, so BigQuery landing is identical.
"""

import random
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional

from .catalog import Catalog
from .events import random_order_event
from .publisher import BasePublisher

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_CANCELLED = "cancelled"
STATUS_ERROR = "error"


@dataclass(frozen=True)
class SpikeConfig:
    num_orders: int = 200
    duration_seconds: float = 60.0
    marketplace_share: float = 0.4
    max_lines: int = 3
    seed: Optional[int] = None

    def validate(self, max_orders: int, max_duration_seconds: float) -> "SpikeConfig":
        if not 1 <= self.num_orders <= max_orders:
            raise ValueError(f"num_orders must be between 1 and {max_orders}")
        if not 0 <= self.duration_seconds <= max_duration_seconds:
            raise ValueError(f"duration_seconds must be between 0 and {max_duration_seconds}")
        if not 0.0 <= self.marketplace_share <= 1.0:
            raise ValueError("marketplace_share must be between 0 and 1")
        return self


class SpikeJob:
    def __init__(
        self,
        catalog: Catalog,
        publisher: BasePublisher,
        config: SpikeConfig,
        clock: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ):
        self.spike_id = f"SPIKE-{uuid.uuid4().hex[:8].upper()}"
        self.catalog = catalog
        self.publisher = publisher
        self.config = config
        self._clock = clock
        self._now = now
        self._lock = threading.Lock()
        self._cancel = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.status = STATUS_PENDING
        self.submitted = 0
        self.published = 0
        self.failed = 0
        self.by_channel: Counter = Counter()
        self.order_total = 0.0
        self.errors: List[str] = []
        self.recent_events: List[Dict] = []
        self.started_at: Optional[datetime] = None
        self.finished_at: Optional[datetime] = None
        self._t0 = 0.0
        self._t_end = 0.0

    def start(self) -> "SpikeJob":
        self._thread = threading.Thread(target=self.run, name=self.spike_id, daemon=True)
        self._thread.start()
        return self

    def cancel(self) -> None:
        self._cancel.set()

    def join(self, timeout: Optional[float] = None) -> None:
        if self._thread:
            self._thread.join(timeout)

    @property
    def is_running(self) -> bool:
        return self.status in (STATUS_PENDING, STATUS_RUNNING)

    def _on_done(self, event: Dict, fut: Future) -> None:
        exc = fut.exception()
        with self._lock:
            if exc is None:
                self.published += 1
                self.by_channel[event["channel"]] += 1
                self.order_total += event["order_total"]
                self.recent_events = (self.recent_events + [event])[-10:]
            else:
                self.failed += 1
                if len(self.errors) < 5:
                    self.errors.append(f"{exc.__class__.__name__}: {exc}"[:300])

    def run(self) -> None:
        cfg = self.config
        rng = random.Random(cfg.seed)
        offsets = sorted(rng.uniform(0, cfg.duration_seconds) for _ in range(cfg.num_orders))
        futures: List[Future] = []
        with self._lock:
            self.status = STATUS_RUNNING
            self.started_at = self._now()
            self._t0 = self._clock()
        try:
            i = 0
            while i < len(offsets) and not self._cancel.is_set():
                elapsed = self._clock() - self._t0
                if offsets[i] > elapsed:
                    self._cancel.wait(min(offsets[i] - elapsed, 0.5))
                    continue
                batch = []
                while i < len(offsets) and offsets[i] <= elapsed and len(batch) < 100:
                    ts = self.started_at + timedelta(seconds=offsets[i])
                    batch.append(random_order_event(
                        self.catalog, rng, order_ts=ts, marketplace_share=cfg.marketplace_share,
                        max_lines=cfg.max_lines, spike_id=self.spike_id,
                    ))
                    i += 1
                for event, fut in zip(batch, self.publisher.publish_many(batch)):
                    futures.append(fut)
                    fut.add_done_callback(lambda f, e=event: self._on_done(e, f))
                with self._lock:
                    self.submitted += len(batch)
            self.publisher.flush()
            for fut in futures:
                try:
                    fut.exception(timeout=60)
                except Exception:
                    pass
            with self._lock:
                self.status = STATUS_CANCELLED if self._cancel.is_set() else STATUS_DONE
        except Exception as exc:
            with self._lock:
                self.status = STATUS_ERROR
                self.errors.append(f"{exc.__class__.__name__}: {exc}"[:300])
        finally:
            with self._lock:
                self.finished_at = self._now()
                self._t_end = self._clock()

    def snapshot(self) -> Dict:
        with self._lock:
            end = self._t_end if self.finished_at else self._clock()
            elapsed = max(0.0, end - self._t0) if self.started_at else 0.0
            return {
                "spike_id": self.spike_id,
                "status": self.status,
                "total": self.config.num_orders,
                "submitted": self.submitted,
                "published": self.published,
                "failed": self.failed,
                "by_channel": dict(self.by_channel),
                "order_total": round(self.order_total, 2),
                "elapsed_seconds": round(elapsed, 1),
                "events_per_second": round(self.published / elapsed, 1) if elapsed > 0 else 0.0,
                "errors": list(self.errors),
                "recent_events": list(self.recent_events),
                "started_at": self.started_at,
                "finished_at": self.finished_at,
            }
