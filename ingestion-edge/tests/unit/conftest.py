# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from ingestion_edge.util import AsyncioBatch
from google.cloud.pubsub_v1 import PublisherClient
from persistqueue import SQLiteAckQueue
from sanic import Sanic
import grpc
import os
import pytest


@pytest.fixture
def app() -> Sanic:
    return Sanic()


@pytest.fixture
def client() -> PublisherClient:
    if "PUBSUB_EMULATOR_HOST" not in os.environ:
        client = PublisherClient(channel=grpc.insecure_channel(target=""))
    else:
        client = PublisherClient()
    client._batch_class = AsyncioBatch
    return client


@pytest.fixture
def q() -> SQLiteAckQueue:
    return SQLiteAckQueue(":memory:")
