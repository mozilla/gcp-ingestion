# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import datetime
from dateutil.parser import parse
from flask.testing import FlaskClient
from ingestion_edge.config import Route, ROUTE_TABLE
from typing import Dict, List, Tuple
from unittest.mock import patch
import pytest


def test_dockerflow(client: FlaskClient):
    r = client.get("/__heartbeat__")
    assert r.status_code == 200
    assert r.json == {"checks": {}, "details": {}, "status": "ok"}

    r = client.get("/__lbheartbeat__")
    assert r.status_code == 200
    assert not r.data

    r = client.get("/__version__")
    assert r.status_code == 200
    assert sorted(r.json.keys()) == ["build", "commit", "source", "version"]


@pytest.mark.parametrize('route', ROUTE_TABLE)
def test_publish(client: FlaskClient, route: Route):
    messages: List[Tuple[str, bytes, Dict[str, str]]] = []

    def _publish(*args):
        messages.append(args)

    with patch('ingestion_edge.publish._publish', new=_publish):
        # test route
        for method in route.methods:
            # submit request
            uri = route.rule.replace("<path:suffix>", "test")
            req_data = "test"
            req_time = datetime.utcnow()
            r = getattr(client, method.lower())(uri, data=req_data)
            assert r.status_code == 200

            # validate message
            assert len(messages) == 1
            topic, rec_data, attrs = messages.pop()
            assert topic == route.topic
            assert rec_data == b"test"
            assert "submission_timestamp" in attrs
            rec_time = parse(attrs.pop("submission_timestamp")[:-1])
            assert (req_time - rec_time).total_seconds() < 1
            assert attrs == {
                "args": "",
                "content_length": str(len(req_data)),
                "host": "localhost",
                "method": method.upper(),
                "protocol": "http",
                "remote_addr": "127.0.0.1",
                "uri": uri,
                "user_agent": client.environ_base['HTTP_USER_AGENT'],
            }
