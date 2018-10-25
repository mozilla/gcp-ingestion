# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import datetime
from dateutil.parser import parse
from sanic.testing import SanicTestClient
from ingestion_edge.config import Route, ROUTE_TABLE
from typing import Dict, List, Tuple
from unittest.mock import patch
import pytest


def test_heartbeat(client: SanicTestClient):
    _, r = client.get("/__heartbeat__")
    r.raise_for_status()
    assert r.json == {
        "checks": {"check_disk_bytes_free": "ok"},
        "details": {},
        "status": "ok",
    }


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
