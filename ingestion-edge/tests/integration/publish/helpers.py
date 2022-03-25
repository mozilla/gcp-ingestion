"""Reusable code for integration tests against publish endpoints."""

from dataclasses import dataclass, field
from google.cloud.pubsub_v1 import SubscriberClient
from google.api_core.retry import Retry
from ingestion_edge.config import (
    FLUSH_SLEEP_SECONDS,
    METADATA_HEADERS,
    PUBLISH_TIMEOUT_SECONDS,
)
from typing import Dict, Optional
import dateutil.parser
import requests


@dataclass
class IntegrationTest:
    """Reusable code for a single integration test."""

    # fixtures
    method: str
    server: str
    requests_session: requests.Session
    subscriber: SubscriberClient
    subscription: str
    uri_template: str
    uses_cluster: bool
    # test specific
    args: str = ""
    data: bytes = b""
    headers: Dict[str, Optional[bytes]] = field(default_factory=dict)
    protocol: str = "HTTP/1.1"
    uri_suffix: str = "x"  # may 404 on ""

    @property
    def host(self) -> str:
        """Extract host from server."""
        return self.server.split("://", 1).pop()

    @property
    def uri(self) -> str:
        """Generate uri from uri_template and uri_suffix."""
        return self.uri_template.replace("<suffix:path>", self.uri_suffix)

    def assert_queue_empty(self):
        """Assert queue is empty if server is not a cluster."""
        if not self.uses_cluster:
            status = self.requests_session.get(self.server + "/__heartbeat__").json()
            # queue size goes up to info when not empty
            assert status["checks"]["check_queue_size"] == "ok"

    def assert_queue_not_empty(self):
        """Assert queue is empty if server is not a cluster."""
        if not self.uses_cluster:
            status = self.requests_session.get(self.server + "/__heartbeat__").json()
            # queue size goes up to info when not empty
            assert status["checks"]["check_queue_size"] == "info"

    def assert_accepted(self):
        """Submit message and assert that it was accepted."""
        self.requests_session.headers = self.headers
        response = self.requests_session.request(
            self.method,
            self.server + self.uri + ("" if not self.args else "?" + self.args),
            data=self.data,
        )
        assert response.status_code == 200
        assert response.text == ""

    def assert_rejected(self, status=400):
        """Submit message and assert that it was accepted."""
        if self.headers is not None:
            self.requests_session.headers = self.headers
        response = self.requests_session.request(
            self.method,
            self.server + self.uri + ("" if not self.args else "?" + self.args),
            data=self.data,
        )
        assert response.status_code == status

    def assert_flushed(self):
        """Wait for flush then assert queue empty and message delivered."""
        retry = Retry(
            lambda e: isinstance(e, AssertionError),
            initial=PUBLISH_TIMEOUT_SECONDS,
            multiplier=1,
            # wait up to two flush cycles, with one second extra overhead per flush
            deadline=(FLUSH_SLEEP_SECONDS + PUBLISH_TIMEOUT_SECONDS) * 2,
        )
        if self.uses_cluster:
            # detect flush from delivered
            retry(self.assert_delivered)()
        else:
            # detect flush from heartbeat and validate delivered
            retry(self.assert_queue_empty)()
            self.assert_delivered()

    def assert_delivered(self, count: int = 1):
        """Assert message delivered to PubSub matches message sent."""
        # receive up to two messages
        messages = self.subscriber.pull(
            subscription=self.subscription,
            max_messages=count + 1,
            return_immediately=True,
        ).received_messages
        # assert exactly count message were pulled
        assert len(messages) >= count
        # ack the messages
        self.subscriber.acknowledge(
            subscription=self.subscription, ack_ids=[e.ack_id for e in messages]
        )
        # validate messages
        for message in (e.message for e in messages):
            # validate data
            assert message.data == self.data
            # create dict of attributes
            attributes = dict(message.attributes)
            # varies based on configuration
            assert attributes.pop("remote_addr")
            # determined on the server by stdlib so only validate that it parses
            assert dateutil.parser.parse(attributes.pop("submission_timestamp")[:-1])
            # content length if not overridden
            if ("content-length" in METADATA_HEADERS) and (
                (self.method.upper() != "GET") or self.data
            ):
                assert "content_length" in attributes
                assert attributes.pop("content_length") == str(len(self.data))
            if ":" in self.host:
                assert attributes.pop("host").split(":")[0] == self.host.split(":")[0]
            else:
                assert attributes.pop("host") == self.host
            # validate attributes
            assert attributes == dict(
                # required attributes
                args=self.args,
                method=self.method.upper(),
                protocol=self.protocol,
                uri=self.uri,
                # optional attributes
                **{
                    METADATA_HEADERS.get(key, key): value.decode("latin")
                    for key, value in self.headers.items()
                    if value is not None
                }
            )

    def assert_not_delivered(self):
        """Assert message not delivered to PubSub."""
        messages = self.subscriber.pull(
            subscription=self.subscription, max_messages=1, return_immediately=True
        ).received_messages
        assert list(messages) == []

    def assert_accepted_and_delivered(self):
        """Submit message and assert that it was delivered directly to PubSub."""
        self.assert_accepted()
        self.assert_queue_empty()
        self.assert_delivered()

    def assert_accepted_and_queued(self):
        """Submit message and assert that it was queued locally."""
        self.assert_accepted()
        self.assert_queue_not_empty()
        self.assert_not_delivered()
