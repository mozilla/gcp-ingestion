from google.cloud.pubsub_v1 import PublisherClient
from persistqueue import SQLiteAckQueue
from sanic import Sanic
import grpc
import os
import pytest


@pytest.fixture
def app() -> Sanic:
    return Sanic(name="test", register=False)


@pytest.fixture
def client() -> PublisherClient:
    if "PUBSUB_EMULATOR_HOST" not in os.environ:
        client = PublisherClient(channel=grpc.insecure_channel(target=""))
    else:
        client = PublisherClient()
    return client


@pytest.fixture(autouse=True)
def bad_pubsub():
    if "PUBSUB_EMULATOR_HOST" not in os.environ:
        os.environ["PUBSUB_EMULATOR_HOST"] = "."
        yield
        del os.environ["PUBSUB_EMULATOR_HOST"]
    else:
        yield


@pytest.fixture
def q() -> SQLiteAckQueue:
    return SQLiteAckQueue(":memory:")
