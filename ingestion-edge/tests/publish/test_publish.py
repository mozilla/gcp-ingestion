# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from dataclasses import dataclass
from google.api_core.exceptions import ServerError
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.exceptions import PublishError
from google.cloud.pubsub_v1.types import BatchSettings
from ingestion_edge.publish import _publish
from unittest.mock import patch
from typing import List
import pytest


@dataclass
class PublishResponse:
    message_ids: List[str]


@pytest.fixture
def client():
    return PublisherClient(BatchSettings(max_bytes=1, max_latency=1e-3, max_messages=1))


async def test_ok(client: PublisherClient):
    calls = []

    def api_publish(topic, messages):
        calls.append((topic, messages))
        return PublishResponse(list(range(len(messages))))

    with patch.object(client.api, "publish", new=api_publish):
        message_id = await _publish(client, "topic", b"data", {"attr": "value"})

    assert message_id == 0
    assert len(calls) == 1
    for topic, messages in calls:
        assert topic == "topic"
        assert len(messages) == 1
        message = messages.pop()
        assert message.data == b"data"
        assert message.attributes == {"attr": "value"}


async def test_publish_error_and_recover(client: PublisherClient):
    calls = []

    def api_publish(topic, messages):
        calls.append((topic, messages))
        if len(calls) % 2 == 0:
            return PublishResponse(list(range(len(messages))))
        else:
            return PublishResponse([])

    with patch.object(client.api, "publish", new=api_publish):
        message_id = await _publish(client, "topic", b"data", {"attr": "value"})

    assert message_id == 0
    assert len(calls) == 2
    for topic, messages in calls:
        assert topic == "topic"
        assert len(messages) == 1
        message = messages.pop()
        assert message.data == b"data"
        assert message.attributes == {"attr": "value"}


async def test_publish_error_and_propagate(client: PublisherClient):
    calls = []

    def api_publish(topic, messages):
        calls.append((topic, messages))
        return PublishResponse([])

    with pytest.raises(PublishError):
        with patch.object(client.api, "publish", new=api_publish):
            await _publish(client, "topic", b"data", {"attr": "value"})

    assert len(calls) == 2
    for topic, messages in calls:
        assert topic == "topic"
        assert len(messages) == 1
        message = messages.pop()
        assert message.data == b"data"
        assert message.attributes == {"attr": "value"}


async def test_other_error(client: PublisherClient):
    calls = []

    def api_publish(topic, messages):
        calls.append((topic, messages))
        raise ServerError("test")

    with pytest.raises(ServerError):
        with patch.object(client.api, "publish", new=api_publish):
            await _publish(client, "topic", b"data", {"attr": "value"})

    assert len(calls) == 1
    for topic, messages in calls:
        assert topic == "topic"
        assert len(messages) == 1
        message = messages.pop()
        assert message.data == b"data"
        assert message.attributes == {"attr": "value"}
