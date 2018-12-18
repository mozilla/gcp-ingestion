# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.types import PublishResponse
from ingestion_edge import flush
from persistqueue import SQLiteAckQueue
from pytest_mock import MockFixture
from sanic import Sanic
from socket import socket
import asyncio


def test_init_app(
    app: Sanic, client: PublisherClient, mocker: MockFixture, q: SQLiteAckQueue
):
    _sleep = asyncio.sleep
    sleep = mocker.patch.object(asyncio, "sleep", return_value=_sleep(0))
    # don't hit actual pubsub
    publish = mocker.patch.object(
        client.api, "publish", return_value=PublishResponse(message_ids=["1"])
    )

    # listener to create test conditions while sanic is running
    @app.listener("after_server_start")
    async def after_server_start(app, _):
        # wait for background task to sleep once
        while not sleep.called:
            await _sleep(0.01)
        # queue a message
        q.put(("topic", b"data", {}))
        # wait for message to be published
        while q.size > 0 or q.unack_count() > 0:
            await _sleep(0.01)
        # stop Sanic
        app.stop()
        # queue message to be delivered on shutdown
        q.put(("topic", b"data", {}))
        q.put(("topic", b"data", {}))

    # set required configuration
    app.config.update(
        FLUSH_CONCURRENT_BYTES=1, FLUSH_CONCURRENT_MESSAGES=1, FLUSH_SLEEP_SECONDS=0.01
    )
    # configure sanic listeners to handle q in the background
    flush.init_app(app, client, q)
    # use a socket to bind to a random port and allow parallel testing
    sock = socket()
    sock.bind(("", 0))
    # start the app
    app.run(sock=sock)
    # make sure everything flushed cleanly
    assert q.size == 0
    assert q.unack_count() == 0
    # make sure publish was called the expected number of times
    assert publish.call_count == 3
