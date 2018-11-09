# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from dataclasses import dataclass
from ingestion_edge.dockerflow import check_disk_bytes_free
from typing import Any, Dict
from unittest.mock import patch
import pytest


@dataclass
class MockStat:
    f_bfree: int = 0
    f_frsize: int = 0


class MockApp:
    def __init__(self, minimum_disk_bytes_free: int = 1, queue_path: str = "queue"):
        self.config: Dict[str, Any] = {}
        if queue_path is not None:
            self.config["QUEUE_PATH"] = queue_path
        if minimum_disk_bytes_free is not None:
            self.config["MINIMUM_DISK_FREE_BYTES"] = minimum_disk_bytes_free


@pytest.mark.parametrize("mock_app_args", [(None, None), (1, None), (None,)])
def test_noop(mock_app_args):
    app = MockApp(*mock_app_args)
    statvfs_calls = []

    def statvfs(path):
        statvfs_calls.append(path)
        return MockStat()

    with patch("os.statvfs", new=statvfs):
        status = check_disk_bytes_free(app)

    assert len(statvfs_calls) == 0
    assert status == []


def test_heartbeat_ok():
    app = MockApp()
    statvfs_calls = []

    def statvfs(path):
        statvfs_calls.append(path)
        return MockStat(1, 1)

    with patch("os.statvfs", new=statvfs):
        status = check_disk_bytes_free(app)

    assert len(statvfs_calls) == 1
    assert statvfs_calls.pop() is app.config["QUEUE_PATH"]
    assert status == []


def test_heartbeat_warn():
    app = MockApp()
    statvfs_calls = []

    def statvfs(path):
        statvfs_calls.append(path)
        raise FileNotFoundError()

    with patch("os.statvfs", new=statvfs):
        status = check_disk_bytes_free(app)

    assert len(statvfs_calls) == 1
    assert statvfs_calls.pop() is app.config["QUEUE_PATH"]
    assert len(status) == 1
    error = status.pop()
    assert error.id == "edge.checks.W001"
    assert error.level == 30
    assert error.msg == "queue path does not exist"


def test_heartbeat_error():
    app = MockApp()
    statvfs_calls = []

    def statvfs(path):
        statvfs_calls.append(path)
        return MockStat()

    with patch("os.statvfs", new=statvfs):
        status = check_disk_bytes_free(app)

    assert len(statvfs_calls) == 1
    assert statvfs_calls.pop() is app.config["QUEUE_PATH"]
    assert len(status) == 1
    error = status.pop()
    assert error.id == "edge.checks.E001"
    assert error.level == 40
    assert error.msg == "disk bytes free below threshold"
