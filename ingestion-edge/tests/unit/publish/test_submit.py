from dataclasses import dataclass, field
from datetime import datetime
from dateutil.parser import parse
from ingestion_edge import publish
from multidict import CIMultiDict
from sqlite3 import DatabaseError
from unittest.mock import MagicMock
from sanic.response import HTTPResponse
from typing import Any, Dict, Tuple
import asyncio
import pytest


@dataclass
class MockRequest:
    body: bytes = b"body"
    headers: CIMultiDict = field(default_factory=lambda: CIMultiDict(header="header"))
    host: str = "host"
    ip: str = "ip"
    method: str = "method"
    path: str = "path"
    query_string: str = "query_string"
    version: str = "version"
    head: bytes = b""


class ListQueue(list):
    def put(self, item):
        self.append(item)


async def call_submit(client=None, q=None, **kwargs) -> Tuple[datetime, HTTPResponse]:
    return (
        datetime.utcnow(),
        await publish.submit(
            request=MockRequest(**kwargs),  # type: ignore
            client=client,
            timeout=None,
            q=q,
            topic="topic",
            metadata_headers={"header": "header"},
        ),
    )


def validate(start_time: datetime, response: HTTPResponse, q: ListQueue):
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
        "protocol": "HTTP/version",
        "remote_addr": "ip",
        "uri": "path",
    }


@pytest.fixture
async def client() -> MagicMock:
    client = MagicMock()
    client.publish.return_value = asyncio.Future()
    return client


async def test_ok(client: MagicMock):
    client.publish.return_value.set_result(None)
    start_time, response = await call_submit(client)
    q = ListQueue([call[0] + (call[1],) for call in client.publish.call_args_list])
    validate(start_time, response, q)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"host": "a" * 1025},
        {"query_string": "a" * 1025},
        {"path": "a" * 1025},
        {"headers": CIMultiDict(header="a" * 1025)},
        {"headers": CIMultiDict(header="\xff" * 513)},
    ],
)
async def test_invalid(kwargs: Dict[str, Any]):
    _, response = await call_submit(**kwargs)
    assert response.status == 431
    assert response.body == b"header too large\n"


async def test_database_error(client: MagicMock):
    q = MagicMock()
    q.put.side_effect = DatabaseError()
    client.publish.return_value.set_exception(TimeoutError())
    _, response = await call_submit(client, q)
    assert response.status == 507
    assert response.body == b""


async def test_pubsub_error(client: MagicMock):
    client.publish.return_value.set_exception(Exception())
    q = ListQueue([])
    start_time, response = await call_submit(client, q)
    validate(start_time, response, q)
