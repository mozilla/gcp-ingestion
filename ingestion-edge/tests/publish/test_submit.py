# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from dataclasses import dataclass
from datetime import datetime
from dateutil.parser import parse
from google.cloud.pubsub_v1.publisher.exceptions import PublishError
from ingestion_edge import publish
from multidict import CIMultiDict
from sqlite3 import DatabaseError
from unittest.mock import patch
import google.api_core.exceptions
import pytest


@dataclass
class MockRequest:
    body: bytes = b"body"
    headers: CIMultiDict = CIMultiDict(header="header")
    host: str = "host"
    ip: str = "ip"
    method: str = "method"
    path: str = "path"
    query_string: str = "query_string"
    scheme: str = "scheme"


class ListQueue(list):
    def put(self, item):
        self.append(item)


async def call_submit(q=None):
    return (
        datetime.utcnow(),
        await publish.submit(
            request=MockRequest(),
            client=None,
            q=q,
            topic="topic",
            metadata_headers={"header": "header"},
        ),
    )


def validate(start_time, response, q):
    # validate response
    assert response.status == 200
    assert response.body == b""

    # validate message
    assert len(q) == 1
    topic, data, attrs = q.pop()
    assert topic == "topic"
    assert data == b"body"
    assert "submission_timestamp" in attrs
    submit_time = parse(attrs.pop("submission_timestamp")[:-1])
    assert (start_time - submit_time).total_seconds() < 1
    assert attrs == {
        "args": "query_string",
        "header": "header",
        "host": "host",
        "method": "method",
        "protocol": "scheme",
        "remote_addr": "ip",
        "uri": "path",
    }


async def test_ok():
    q = ListQueue([])

    async def _publish(_, *args):
        q.put(args)
        return len(q)

    with patch.object(publish, "_publish", new=_publish):
        start_time, response = await call_submit()

    validate(start_time, response, q)


async def test_publish_error():
    async def _publish(*_):
        raise PublishError(None)

    q = ListQueue([])

    with patch.object(publish, "_publish", new=_publish):
        _, response = await call_submit(q=q)

    assert len(q) == 0
    assert response.status == 400
    assert response.body == b""


async def test_database_error():
    def put(*_):
        raise DatabaseError

    async def _publish(*_):
        raise TimeoutError

    q = ListQueue([])
    q.put = put

    with patch.object(publish, "_publish", new=_publish):
        _, response = await call_submit(q=q)

    assert len(q) == 0
    assert response.status == 507
    assert response.body == b""


TRANSIENT_ERRORS = (
    google.api_core.exceptions.Aborted("test"),
    google.api_core.exceptions.Cancelled("test"),
    google.api_core.exceptions.Forbidden("test"),
    google.api_core.exceptions.RetryError("test", None),
    google.api_core.exceptions.ServerError("test"),
    google.api_core.exceptions.TooManyRequests("test"),
    TimeoutError("test"),
)


@pytest.mark.parametrize("error", TRANSIENT_ERRORS)
async def test_transient_error(error):
    async def _publish(*_):
        raise error

    q = ListQueue([])

    with patch.object(publish, "_publish", new=_publish):
        start_time, response = await call_submit(q=q)

    validate(start_time, response, q)
