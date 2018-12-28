# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
"""Reusable code for integration tests against publish endpoints."""

from dataclasses import dataclass, field
from google.cloud.pubsub_v1 import SubscriberClient
from ingestion_edge.config import METADATA_HEADERS
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
    protocol: str = "http"
    uri_suffix: str = "."  # may 404 on ""

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

    def assert_delivered(self):
        """Assert message delivered to PubSub matches message sent."""
        # receive up to two messages
        received_messages = self.subscriber.pull(
            self.subscription, 2, True, retry=None
        ).received_messages
        # assert exactly one message was pulled
        assert len(received_messages) == 1
        # ack the message
        self.subscriber.acknowledge(self.subscription, [received_messages[0].ack_id])
        # validate data
        assert received_messages[0].message.data == self.data
        # create dict of attributes
        attributes = dict(received_messages[0].message.attributes)
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
        # validate attributes
        assert attributes == dict(
            # required attributes
            args=self.args,
            host=self.host,
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
        messages = self.subscriber.pull(self.subscription, 1, True).received_messages
        assert list(messages) == []

    def assert_accepted_and_delivered(self):
        """Submit message and assert that it was delivered directly to PubSub."""
        self.assert_accepted()
        self.assert_queue_empty()
        self.assert_delivered()
