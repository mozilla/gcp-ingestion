from .helpers import IntegrationTest
from ingestion_edge.config import PUBLISH_TIMEOUT_SECONDS
from google.cloud.pubsub_v1 import PublisherClient
from typing import Any
import pytest


def test_submit_pubsub_server_error(
    integration_test: IntegrationTest,
    publisher: PublisherClient,
    pubsub: Any,
    topic: str,
):
    if pubsub == "google":
        pytest.skip("requires pubsub emulator")

    # override pubsub status
    publisher.update_topic({"name": topic}, {"paths": ["status_code=internal"]})
    integration_test.assert_accepted_and_queued()

    # restore pubsub status
    publisher.update_topic({"name": topic}, {"paths": ["status_code="]})
    integration_test.assert_flushed()


def test_submit_pubsub_timeout(
    integration_test: IntegrationTest,
    publisher: PublisherClient,
    pubsub: Any,
    topic: str,
):
    if pubsub == "google":
        pytest.skip("requires pubsub emulator")

    # override pubsub response time
    publisher.update_topic(
        {"name": topic}, {"paths": ["sleep=%.1f" % (PUBLISH_TIMEOUT_SECONDS + 1)]}
    )
    integration_test.assert_accepted_and_queued()

    # restore pubsub response time
    publisher.update_topic({"name": topic}, {"paths": ["sleep="]})
    integration_test.assert_flushed()
