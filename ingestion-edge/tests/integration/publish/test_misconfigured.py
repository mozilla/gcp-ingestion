from .helpers import IntegrationTest
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
import pytest


def test_submit_pubsub_permission_denied(
    integration_test: IntegrationTest,
    publisher: PublisherClient,
    pubsub: str,
    topic: str,
):
    if pubsub == "google":
        pytest.skip("not implemented")
    else:
        publisher.update_topic(
            {"name": topic}, {"paths": ["status_code=permission_denied"]}
        )
    integration_test.assert_accepted_and_queued()
    if pubsub == "google":
        pytest.skip("not implemented")
    else:
        publisher.update_topic({"name": topic}, {"paths": ["status_code="]})
    integration_test.assert_flushed()


def test_submit_pubsub_topic_not_found(
    integration_test: IntegrationTest,
    publisher: PublisherClient,
    subscriber: SubscriberClient,
    subscription: str,
    topic: str,
):
    publisher.delete_topic(topic)
    try:
        integration_test.assert_accepted_and_queued()
    finally:
        subscriber.delete_subscription(subscription)
        publisher.create_topic(topic)
        subscriber.create_subscription(subscription, topic)
    integration_test.assert_flushed()
