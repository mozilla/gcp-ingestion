# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from dataclasses import dataclass
from ingestion_edge.dockerflow import check_disk_bytes_free
from typing import Optional
from unittest.mock import patch
import pytest


@dataclass
class Stat:
    f_bfree: int = 1
    f_frsize: int = 1


@dataclass
class Queue:
    path: Optional[str] = "path"


@dataclass
class App:
    threshold: Optional[int] = 1

    @property
    def config(self):
        return (
            {}
            if self.threshold is None
            else {"MINIMUM_DISK_FREE_BYTES": self.threshold}
        )


@pytest.mark.parametrize(
    "app,q",
    [
        (app, q)
        for app in (App(None), App())
        for q in (None, Queue(None), Queue(":memory:"), Queue())
    ][:-1],
)
def test_noop(app, q):
    statvfs_calls = []

    def statvfs(path):
        statvfs_calls.append(path)

    with patch("os.statvfs", new=statvfs):
        status = check_disk_bytes_free(app, q)

    assert statvfs_calls == []
    assert status == []


def test_ok():
    statvfs_calls = []

    def statvfs(path):
        statvfs_calls.append(path)
        return Stat()

    with patch("os.statvfs", new=statvfs):
        status = check_disk_bytes_free(App(), Queue())

    assert statvfs_calls == ["path"]
    assert status == []


def test_warn():
    statvfs_calls = []

    def statvfs(path):
        statvfs_calls.append(path)
        raise FileNotFoundError()

    with patch("os.statvfs", new=statvfs):
        status = check_disk_bytes_free(App(), Queue())

    assert statvfs_calls == ["path"]
    assert len(status) == 1
    error = status.pop()
    assert error.id == "edge.checks.W001"
    assert error.level == 30
    assert error.msg == "queue path does not exist"


def test_error():
    statvfs_calls = []

    def statvfs(path):
        statvfs_calls.append(path)
        return Stat(0, 0)

    with patch("os.statvfs", new=statvfs):
        status = check_disk_bytes_free(App(), Queue())

    assert statvfs_calls == ["path"]
    assert len(status) == 1
    error = status.pop()
    assert error.id == "edge.checks.E001"
    assert error.level == 40
    assert error.msg == "disk bytes free below threshold"
