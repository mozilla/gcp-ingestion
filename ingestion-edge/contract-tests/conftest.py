# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

# importing from private module _pytest for types only
import _pytest.fixtures
import _pytest.config.argparsing
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
        "--no-create-pubsub-resources",
        action="store_false",
        dest="create_pubsub_resources",
        default=True,
        help="Don't create PubSub resources for tests",
    )
    parser.addoption(
        "--no-verify",
        action="store_false",
        dest="verify",
        default=None,
        help="Don't verify SSL certs",
    )


@pytest.fixture
def create_pubsub_resources(request: _pytest.fixtures.SubRequest) -> bool:
    return request.config.getoption("create_pubsub_resources")


@pytest.fixture
def server(request: _pytest.fixtures.SubRequest) -> str:
    return request.config.getoption("server")


@pytest.fixture
def requests_session(request: _pytest.fixtures.SubRequest) -> requests.Session:
    session = requests.Session()
    session.verify = request.config.getoption("verify")
    return session
