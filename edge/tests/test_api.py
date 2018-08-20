# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import datetime
from dateutil.parser import parse
from typing import List
import requests


def test_dockerflow(requests_session: requests.Session, server: str):
    r = requests_session.get(server + "/__heartbeat__")
    r.raise_for_status()
    assert r.json() == {"checks": {}, "details": {}, "status": "ok"}

    r = requests_session.get(server + "/__lbheartbeat__")
    r.raise_for_status()
    assert r.text == ""

    r = requests_session.get(server + "/__version__")
    r.raise_for_status()
    assert sorted(r.json().keys()) == ["build", "commit", "source", "version"]


def test_publish(
    create_pubsub_resources: bool,
    requests_session: requests.Session,
    server: str,
):
    from edge.conf import ROUTE_TABLE
    from edge.publish import client as publisher
    from google.cloud.pubsub import SubscriberClient

    subscriber = SubscriberClient()

    def topic_to_subscription(topic: str):
        return topic.replace("/topics/", "/subscriptions/test_")

    # create topics and subscriptions if they don't yet exist
    if create_pubsub_resources:
        topics = set(map(lambda route: route[1], ROUTE_TABLE))

        # get list of topics that already exist
        projects = {route[1].split("/topics/", 1)[0] for route in ROUTE_TABLE}
        all_topics: List[str] = []
        for project in projects:
            all_topics += [t.name for t in publisher.list_topics(project)]
        existing_topics = set(all_topics).intersection(topics)

        # create new subscriptions to existing topics if needed
        for topic in existing_topics:
            subscription = topic_to_subscription(topic)
            if subscription not in publisher.list_topic_subscriptions(topic):
                subscriber.create_subscription(subscription, topic)

        # create new topics and subscriptions if needed
        for topic in topics - existing_topics:
            publisher.create_topic(topic)
            subscription = topic_to_subscription(topic)
            subscriber.create_subscription(subscription, topic)

    # test route table
    for path, topic, methods in ROUTE_TABLE:
        subscription = topic_to_subscription(topic)
        for method in methods:
            # fast forward subscriber to HEAD
            while True:
                resp = subscriber.pull(
                    subscription,
                    100,
                    timeout=0.1,
                )
                if not resp.received_messages:
                    break
                subscriber.acknowledge(
                    subscription,
                    [message.ack_id for message in resp.received_messages],
                )

            # submit request to edge
            uri = path.replace("<path:suffix>", "test")
            body = "test"
            req_time = datetime.utcnow()
            r = getattr(requests_session, method.lower())(server + uri, body)
            r.raise_for_status()

            # validate message delivered to pubsub
            resp = subscriber.pull(subscription, 1, timeout=0.1)
            assert len(resp.received_messages) == 1
            assert resp.received_messages[0].message.data == b"test"
            attributes = resp.received_messages[0].message.attributes
            expect = {
                "args": "",
                "content_length": str(len(body)),
                "host": server.split("://", 1)[1],
                "method": method,
                "protocol": server.split("://", 1)[0],
                "remote_addr": lambda x: True,
                "submission_timestamp": lambda x: (
                    req_time - parse(x[:-1])
                ).total_seconds() < 1,
                "uri": uri,
                "user_agent": "python-requests/" + requests.__version__,
            }
            for key, validate in expect.items():
                assert key in attributes
                if type(validate) is str:
                    assert attributes[key] == validate
                else:
                    assert validate(attributes[key])
