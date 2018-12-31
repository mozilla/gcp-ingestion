# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

import google.api_core.exceptions
import pytest


@pytest.fixture(autouse=True)
def skipif(pubsub):
    if pubsub == "google":
        pytest.skip("requires pubsub emulator")


def test_emulator(publisher, subscriber):
    publisher.create_topic("topic")
    subscriber.create_subscription("subscription", "topic")
    messages = [
        {"data": b"data", "attributes": {}},
        {"data": b"", "attributes": {"meta": "data"}},
    ]
    publish_response = publisher.api.publish("topic", messages, retry=None)
    pull_response = subscriber.pull("subscription", 3, True)
    subscriber.acknowledge(
        "subscription", [element.ack_id for element in pull_response.received_messages]
    )
    assert messages == [
        {"data": element.message.data, "attributes": dict(element.message.attributes)}
        for element in pull_response.received_messages
    ]
    assert publish_response.message_ids == [
        element.message.message_id for element in pull_response.received_messages
    ]
    assert [] == list(subscriber.pull("subscription", 0, True).received_messages)
    subscriber.delete_subscription("subscription")
    publisher.delete_topic("topic")


@pytest.mark.parametrize(
    "status_code",
    [
        "CANCELLED",
        "UNKNOWN",
        "INVALID_ARGUMENT",
        "DEADLINE_EXCEEDED",
        "NOT_FOUND",
        "ALREADY_EXISTS",
        "PERMISSION_DENIED",
        "RESOURCE_EXHAUSTED",
        "FAILED_PRECONDITION",
        "ABORTED",
        "OUT_OF_RANGE",
        "DATA_LOSS",
        "UNAUTHENTICATED",
    ],
)
def test_fake_status_code(publisher, status_code):
    expect = getattr(
        google.api_core.exceptions,
        "".join(map(lambda x: x.capitalize(), status_code.split("_"))),
    )
    publisher.update_topic({"name": "topic"}, {"paths": ["status_code=" + status_code]})
    with pytest.raises(expect):
        publisher.api.publish("topic", [{"data": b""}], retry=None)


@pytest.mark.parametrize(
    "status_code,expect",
    [
        ("UNIMPLEMENTED", google.api_core.exceptions.MethodNotImplemented),
        ("INTERNAL", google.api_core.exceptions.InternalServerError),
        ("UNAVAILABLE", google.api_core.exceptions.ServiceUnavailable),
    ],
)
def test_fake_status_code_renamed(publisher, status_code, expect):
    publisher.update_topic({"name": "topic"}, {"paths": ["status_code=" + status_code]})
    with pytest.raises(expect):
        publisher.api.publish("topic", [{"data": b""}], retry=None)
