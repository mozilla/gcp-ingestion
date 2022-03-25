from google.cloud.pubsub_v1.types import PublishResponse
from google.cloud.pubsub_v1 import PublisherClient
from persistqueue import SQLiteAckQueue
from pytest_mock import MockFixture
import asyncio
import ingestion_edge.flush
import pytest
import concurrent.futures


@pytest.fixture
def flush(client: PublisherClient, q: SQLiteAckQueue) -> ingestion_edge.flush.Flush:
    return ingestion_edge.flush.Flush(client, q, 1, 1, 1, True)


def test_empty(
    client: PublisherClient, flush: ingestion_edge.flush.Flush, mocker: MockFixture
):
    m = mocker.patch.object(client, "publish")
    assert asyncio.run(flush._flush()) == 0
    m.assert_not_called()


def test_success(
    client: PublisherClient, flush: ingestion_edge.flush.Flush, mocker: MockFixture
):
    published = []

    def _publish(topic, data, **attributes):
        published.append(
            (topic, [(data, attributes)])
        )
        future = concurrent.futures.Future()
        future.set_result("1")
        return future

    mocker.patch.object(client, "publish", _publish)

    flush.q.put(("topic", b"data", {"attr": "value"}))

    assert asyncio.run(flush._flush()) == 1
    assert flush.q.unack_count() == 0
    assert flush.q.ready_count() == 0
    assert flush.q.acked_count() == 0
    assert flush.q.size == 0
    assert published == [("topic", [(b"data", {"attr": "value"})])]


def test_invalid_message(
    client: PublisherClient, flush: ingestion_edge.flush.Flush, mocker: MockFixture
):
    flush.q.put(("topic", b"data"))
    m = mocker.patch.object(client, "publish")
    with pytest.raises(ValueError):
        asyncio.run(flush._flush())
    m.assert_not_called()
    assert flush.q.unack_count() == 0
    assert flush.q.ready_count() == 1
    assert flush.q.acked_count() == 0
    assert flush.q.size == 1


def test_publish_exception(
    client: PublisherClient, flush: ingestion_edge.flush.Flush, mocker: MockFixture
):
    published = []

    def _publish(topic, data, **attributes):
        published.append(
            (topic, [(data, attributes)])
        )
        raise Exception("test")

    mocker.patch.object(client, "publish", _publish)

    flush.q.put(("topic", b"data", {}))

    with pytest.raises(Exception):
        asyncio.run(flush._flush())

    assert published == [("topic", [(b"data", {})])]
    assert flush.q.unack_count() == 0
    assert flush.q.ready_count() == 1
    assert flush.q.acked_count() == 0
    assert flush.q.size == 1
