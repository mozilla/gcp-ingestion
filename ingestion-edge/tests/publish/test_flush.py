# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from ingestion_edge.publish import flush
from persistqueue.exceptions import Empty
from unittest.mock import patch
import pytest


class MockQueue:
    def __init__(self):
        self.q = []
        self.unacked = []

    @property
    def size(self):
        return len(self.q)

    def put(self, item):
        self.q.append(item)

    def get(self, block=None):
        try:
            item = self.q.pop(0)
        except IndexError:
            raise Empty
        self.unacked.append(item)
        return item

    def nack(self, item):
        self.unacked.remove(item)
        self.q.append(item)

    def ack(self, item):
        self.unacked.remove(item)


async def test_empty():
    q = MockQueue()
    res = await flush(None, None, q)
    assert q.size == 0
    assert q.unacked == []
    assert res.status == 204
    assert res.body == b""


async def test_success():
    q = MockQueue()
    q.put(("topic", "data", {"attr": "value"}))

    async def _publish(client, topic, data, attrs):
        return "message_id"

    with patch("ingestion_edge.publish._publish", new=_publish):
        res = await flush(None, None, q)

    assert q.size == 0
    assert q.unacked == []
    assert res.status == 200
    assert res.body == b'{"done":1,"pending":0}'


async def test_queue_false_empty():
    q = MockQueue()
    q.put(("topic", "data", {"attr": "value"}))
    q_get = q.get
    calls = []

    def get(block=None):
        calls.append(block)
        if len(calls) == 1:
            raise Empty()
        else:
            return q_get(block)

    q.get = get

    async def _publish(client, topic, data, attrs):
        return "message_id"

    with patch("ingestion_edge.publish._publish", new=_publish):
        res = await flush(None, None, q)

    assert q.size == 0
    assert q.unacked == []
    assert res.status == 200
    assert res.body == b'{"done":1,"pending":0}'
    assert calls == [False, False]


async def test_timeout():
    q = MockQueue()
    q.put(("topic", "data", {"attr": "value"}))

    async def _publish(client, topic, data, attrs):
        raise TimeoutError()

    with patch("ingestion_edge.publish._publish", new=_publish):
        res = await flush(None, None, q)

    assert q.size == 1
    assert q.unacked == []
    assert res.status == 504
    assert res.body == b'{"done":0,"pending":1}'


async def test_permanent_exception():
    q = MockQueue()
    q.put(("topic", "data", {"attr": "value"}))

    async def _publish(client, topic, data, attrs):
        raise Exception()

    with pytest.raises(Exception):
        with patch("ingestion_edge.publish._publish", new=_publish):
            await flush(None, None, q)

    assert q.size == 1
    assert q.unacked == []
