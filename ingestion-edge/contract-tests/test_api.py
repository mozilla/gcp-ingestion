# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import datetime
from dateutil.parser import parse
from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub import PublisherClient, SubscriberClient
from ingestion_edge.config import Route, ROUTE_TABLE
import requests
import pytest

created_topics = set()
created_subscriptions = set()
publisher = PublisherClient()
subscriber = SubscriberClient()


def _topic_to_subscription(topic: str):
    """Get the testing subscription name for a topic"""
    return topic.replace("/topics/", "/subscriptions/test_")


def _create_pubsub_resources(topic: str):
    """Create pubsub topic and subscription if they don't exist"""
    if topic not in created_topics:
        try:
            publisher.create_topic(topic)
        except AlreadyExists:
            pass
        created_topics.add(topic)

    subscription = _topic_to_subscription(topic)
    if subscription not in created_subscriptions:
        try:
            subscriber.create_subscription(subscription, topic)
        except AlreadyExists:
            pass
        created_subscriptions.add(subscription)


def test_heartbeat(requests_session: requests.Session, server: str):
    r = requests_session.get(server + "/__heartbeat__")
    r.raise_for_status()
    assert r.json() == {
        "checks": {"check_disk_bytes_free": "ok"},
        "details": {},
        "status": "ok",
    }


def test_lbheartbeat(requests_session: requests.Session, server: str):
    r = requests_session.get(server + "/__lbheartbeat__")
    r.raise_for_status()
    assert r.text == ""


def test_version(requests_session: requests.Session, server: str):
    r = requests_session.get(server + "/__version__")
    r.raise_for_status()
    assert sorted(r.json().keys()) == ["build", "commit", "source", "version"]


@pytest.mark.parametrize("route", ROUTE_TABLE)
def test_publish(
    create_pubsub_resources: bool,
    requests_session: requests.Session,
    server: str,
    route: Route,
):
    # create pubsub resources
    if create_pubsub_resources:
        _create_pubsub_resources(route.topic)

    # test route
    subscription = _topic_to_subscription(route.topic)
    for method in route.methods:
        # fast forward subscriber to HEAD
        while True:
            resp = subscriber.pull(subscription, 100, timeout=0.1)
            if not resp.received_messages:
                break
            subscriber.acknowledge(
                subscription, [message.ack_id for message in resp.received_messages]
            )

        # submit request to edge
        uri = route.uri.replace("<suffix:path>", "test")
        data = "test"
        req_time = datetime.utcnow()
        r = requests_session.request(method, server + uri, data=data)
        r.raise_for_status()

        # validate message delivered to pubsub
        resp = subscriber.pull(subscription, 1, timeout=0.1)
        assert len(resp.received_messages) == 1
        assert resp.received_messages[0].message.data == b"test"
        attrs = resp.received_messages[0].message.attributes
        assert "submission_timestamp" in attrs
        rec_time = parse(attrs.pop("submission_timestamp")[:-1])
        assert (req_time - rec_time).total_seconds() < 1
        assert "remote_addr" in attrs
        assert attrs.pop("remote_addr")
        assert attrs == {
            "args": "",
            "content_length": str(len(data)),
            "host": server.split("://", 1)[1],
            "method": method.upper(),
            "protocol": server.split("://", 1)[0],
            "uri": uri,
            "user_agent": "python-requests/" + requests.__version__,
        }
