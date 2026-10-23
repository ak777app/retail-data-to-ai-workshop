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

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from storefront.catalog import load_sample_catalog  # noqa: E402
from storefront.config import load_config  # noqa: E402


@pytest.fixture
def catalog():
    return load_sample_catalog()


@pytest.fixture
def config(monkeypatch, tmp_path):
    for k in ("PUBLISH_MODE", "CATALOG_SOURCE", "SPIKE_MAX_ORDERS", "SPIKE_DEFAULT_ORDERS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("PROJECT_ID", "test-project")
    monkeypatch.setenv("LOCAL_EVENTS_FILE", str(tmp_path / "events.jsonl"))
    return load_config(env_file=tmp_path / "missing.env")
