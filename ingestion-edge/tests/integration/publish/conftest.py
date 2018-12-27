# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from .helpers import IntegrationTest
from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from ingestion_edge.config import ROUTE_TABLE
from typing import Generator, Tuple
import _pytest.fixtures  # importing from private module _pytest for types only
import pytest
import requests


@pytest.fixture(
    params=[
        (route.uri, route.topic, method)
        for route in ROUTE_TABLE
        for method in route.methods
    ]
)
def route(request: _pytest.fixtures.SubRequest) -> Tuple[str, str, str]:
    return request.param


@pytest.fixture
def topic(
    publisher: PublisherClient, route: Tuple[str, str, str]
) -> Generator[str, None, None]:
    name = route[1]
    try:
        publisher.create_topic(name)
        delete = True
    except AlreadyExists:
        delete = False
    try:
        yield name
    finally:
        if delete:
            publisher.delete_topic(name)


@pytest.fixture
def subscription(
    topic: str, subscriber: SubscriberClient
) -> Generator[str, None, None]:
    name = topic.replace("/topics/", "/subscriptions/")
    try:
        subscriber.create_subscription(name, topic)
        delete = True
    except AlreadyExists:
        delete = False
    try:
        yield name
    finally:
        if delete:
            subscriber.delete_subscription(name)


@pytest.fixture
def integration_test(
    route: Tuple[str, str, str],
    requests_session: requests.Session,
    server: str,
    subscriber: SubscriberClient,
    subscription: str,
    uses_cluster: bool,
) -> IntegrationTest:
    uri_template, _, method = route
    return IntegrationTest(
        method=method,
        requests_session=requests_session,
        server=server,
        subscriber=subscriber,
        subscription=subscription,
        uri_template=uri_template,
        uses_cluster=uses_cluster,
    )
