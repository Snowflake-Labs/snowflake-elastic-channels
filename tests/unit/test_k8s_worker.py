# Copyright 2026 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for the EKS single-worker entrypoint helpers."""

import pytest

from elastic_channels.k8s_worker import _ResettableCounter, _worker_id


def test_worker_id_from_job_completion_index(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "17")
    assert _worker_id() == 17


def test_worker_id_falls_back_to_hostname_ordinal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JOB_COMPLETION_INDEX", raising=False)
    monkeypatch.delenv("ELASTIC_WORKER_ID", raising=False)
    monkeypatch.setenv("HOSTNAME", "elastic-ramp-20260901-1234-42")
    assert _worker_id() == 42


def test_worker_id_falls_back_to_explicit_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JOB_COMPLETION_INDEX", raising=False)
    monkeypatch.setenv("HOSTNAME", "not-an-ordinal-hostname")
    monkeypatch.setenv("ELASTIC_WORKER_ID", "5")
    assert _worker_id() == 5


def test_worker_id_raises_without_any_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JOB_COMPLETION_INDEX", raising=False)
    monkeypatch.delenv("ELASTIC_WORKER_ID", raising=False)
    monkeypatch.setenv("HOSTNAME", "no-ordinal-here")
    with pytest.raises(RuntimeError):
        _worker_id()


def test_resettable_counter_tracks_max_and_resets() -> None:
    counter = _ResettableCounter()
    with counter.get_lock():
        counter.value = max(counter.value, 50)
    with counter.get_lock():
        counter.value = max(counter.value, 30)
    assert counter.value == 50
    with counter.get_lock():
        counter.value = 0
    assert counter.value == 0
