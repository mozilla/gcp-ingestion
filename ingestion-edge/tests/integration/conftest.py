# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient

# importing from private module _pytest for types only
import _pytest.config.argparsing
import _pytest.fixtures
import grpc
import os
import pytest
import requests


def pytest_addoption(parser: _pytest.config.argparsing.Parser):
    parser.addoption(
        "--server",
        dest="server",
        default="http://web:8000",
        help="Server to run tests against",
    )
    parser.addoption(
        "--uses-cluster",
        action="store_true",
        dest="uses_cluster",
        default=False,
        help="Indicate --server has more than one process",
    )
    parser.addoption(
        "--no-verify",
        action="store_false",
        dest="verify",
        default=None,
        help="Don't verify SSL certs",
    )


@pytest.fixture
def pubsub() -> str:
    if "PUBSUB_EMULATOR_HOST" in os.environ:
        return "remote"
    else:
        return "google"


@pytest.fixture
def publisher() -> PublisherClient:
    return PublisherClient()


@pytest.fixture
def subscriber() -> SubscriberClient:
    if "PUBSUB_EMULATOR_HOST" in os.environ:
        host = os.environ["PUBSUB_EMULATOR_HOST"]
        try:
            # PUBSUB_EMULATOR_HOST will override a channel argument
            # so remove it in order to preserve channel options for
            # supporting large messages
            del os.environ["PUBSUB_EMULATOR_HOST"]
            return SubscriberClient(
                channel=grpc.insecure_channel(
                    host, options=[("grpc.max_receive_message_length", -1)]
                )
            )
        finally:
            os.environ["PUBSUB_EMULATOR_HOST"] = host
    else:
        return SubscriberClient()


@pytest.fixture
def server(request: _pytest.fixtures.SubRequest) -> str:
    return request.config.getoption("server")


@pytest.fixture
def requests_session(request: _pytest.fixtures.SubRequest) -> requests.Session:
    session = requests.Session()
    session.verify = request.config.getoption("verify")
    return session


@pytest.fixture
def uses_cluster(request: _pytest.fixtures.SubRequest) -> bool:
    return request.config.getoption("uses_cluster")
