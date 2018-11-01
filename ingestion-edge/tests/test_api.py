# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from dataclasses import dataclass
from datetime import datetime
from dateutil.parser import parse
from sanic.testing import SanicTestClient
from ingestion_edge.config import Route, ROUTE_TABLE
from typing import Dict, List, Tuple
from unittest.mock import patch
import pytest


@dataclass
class FakeStat:
    f_bfree: int = 0
    f_frsize: int = 0


def test_heartbeat_noop(client: SanicTestClient):
    def check(app):
        assert app is client.app
        return []

    with patch("ingestion_edge.dockerflow.check_disk_bytes_free", new=check):
        _, r = client.get("/__heartbeat__")

    assert r.json == {
        "checks": {"check_disk_bytes_free": "ok"},
        "details": {},
        "status": "ok",
    }
    r.raise_for_status()


def test_heartbeat_ok(client: SanicTestClient):
    def statvfs(path):
        assert path == client.app.config["QUEUE_PATH"]
        return FakeStat(1, 1)

    client.app.config["MINIMUM_DISK_FREE_BYTES"] = 1

    with patch("os.statvfs", new=statvfs):
        _, r = client.get("/__heartbeat__")

    assert r.json == {
        "checks": {"check_disk_bytes_free": "ok"},
        "details": {},
        "status": "ok",
    }
    r.raise_for_status()


def test_heartbeat_warn(client: SanicTestClient):
    def statvfs(path):
        assert path == client.app.config["QUEUE_PATH"]
        raise FileNotFoundError()

    client.app.config["MINIMUM_DISK_FREE_BYTES"] = 1

    with patch("os.statvfs", new=statvfs):
        _, r = client.get("/__heartbeat__")

    assert r.json == {
        "checks": {"check_disk_bytes_free": "warning"},
        "details": {
            "check_disk_bytes_free": {
                "level": 30,
                "messages": {"edge.checks.W001": "queue path does not exist"},
                "status": "warning",
            }
        },
        "status": "warning",
    }
    assert r.status == 500


def test_heartbeat_error(client: SanicTestClient):
    def statvfs(path):
        assert path == client.app.config["QUEUE_PATH"]
        return FakeStat()

    client.app.config["MINIMUM_DISK_FREE_BYTES"] = 1

    with patch("os.statvfs", new=statvfs):
        _, r = client.get("/__heartbeat__")

    assert r.json == {
        "checks": {"check_disk_bytes_free": "error"},
        "details": {
            "check_disk_bytes_free": {
                "level": 40,
                "messages": {"edge.checks.E001": "disk bytes free below threshold"},
                "status": "error",
            }
        },
        "status": "error",
    }
    assert r.status == 500


def test_lbheartbeat(client: SanicTestClient):
    _, r = client.get("/__lbheartbeat__")
    r.raise_for_status()
    assert r.body == b""


def test_version(client: SanicTestClient):
    _, r = client.get("/__version__")
    r.raise_for_status()
    assert sorted(r.json.keys()) == ["build", "commit", "source", "version"]


@pytest.mark.parametrize("route", ROUTE_TABLE)
def test_publish(client: SanicTestClient, route: Route):
    messages: List[Tuple[str, bytes, Dict[str, str]]] = []

    async def _publish(*args):
        messages.append(args)
        return len(messages)

    with patch("ingestion_edge.publish._publish", new=_publish):
        # test route
        for method in route.methods:
            # submit request
            uri = route.uri.replace("<suffix:path>", "test")
            req_time = datetime.utcnow()
            req, res = getattr(client, method.lower())(
                uri, data="test", headers={"User-Agent": "py.test"}
            )
            res.raise_for_status()
            assert res.body == b""

            # validate message
            assert len(messages) == 1
            topic, data, attrs = messages.pop()
            assert topic == route.topic
            assert data == req.body
            assert "submission_timestamp" in attrs
            sub_time = parse(attrs.pop("submission_timestamp")[:-1])
            assert (req_time - sub_time).total_seconds() < 1
            assert attrs == {
                "args": "",
                "content_length": str(len(req.body)),
                "host": "127.0.0.1:42101",
                "method": method.upper(),
                "protocol": "http",
                "remote_addr": "127.0.0.1",
                "uri": uri,
                "user_agent": "py.test",
            }


@pytest.mark.parametrize("route", ROUTE_TABLE[:1])
def test_flush(client: SanicTestClient, route: Route):
    async def _timeout(*args):
        raise TimeoutError

    uri = route.uri.replace("<suffix:path>", "test")

    with patch("ingestion_edge.publish._publish", new=_timeout):
        req, res = getattr(client, route.methods[0].lower())(
            uri, data="test", headers={"User-Agent": "py.test"}
        )

    res.raise_for_status()
    assert res.body == b""

    messages: List[Tuple[str, bytes, Dict[str, str]]] = []

    async def _publish(*args):
        messages.append(args)
        return len(messages)

    with patch("ingestion_edge.publish._publish", new=_publish):
        _, res = client.get("/__flush__")

    assert res.status == 200
    assert res.json == {"done": 1, "pending": 0}
    assert len(messages) == 1
    topic, data, attrs = messages.pop()
    assert topic == route.topic
    assert data == req.body
    _, res = client.get("/__flush__")
    assert res.status == 204
    assert res.body == b""
    client.get("/__heartbeat__")[1].raise_for_status()
