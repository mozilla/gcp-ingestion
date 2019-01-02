# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Main logic for handling submit requests."""

from datetime import datetime
from sqlite3 import DatabaseError
from sanic import Sanic, response
from sanic.request import Request
from functools import partial
from google.cloud.pubsub_v1 import PublisherClient
from persistqueue import SQLiteAckQueue
from typing import Dict, Optional, Tuple
from .util import async_wrap, HTTP_STATUS
import asyncio


async def submit(
    request: Request,
    client: PublisherClient,
    q: SQLiteAckQueue,
    topic: str,
    metadata_headers: Dict[str, str],
    timeout: Optional[float],
    **kwargs
) -> response.HTTPResponse:
    """Deliver request to the pubsub topic.

    Deliver to the local queue to be retried on transient errors.
    """
    data = request.body
    attrs = {
        key: value
        for key, value in dict(
            submission_timestamp=datetime.utcnow().isoformat() + "Z",
            uri=request.path,
            protocol="HTTP/" + request.version,
            method=request.method,
            args=request.query_string,
            remote_addr=request.ip,
            host=request.host,
            **{
                attr: request.headers.get(header)
                for header, attr in metadata_headers.items()
            }
        ).items()
        if value is not None
    }
    # assert valid pubsub message
    for value in attrs.values():
        if len(value.encode("utf8")) > 1024:
            # attribute exceeds value size limit of 1024 bytes
            # https://cloud.google.com/pubsub/quotas#resource_limits
            return response.text("header too large\n", 431)
    try:
        future = client.publish(topic, data, **attrs)
        await asyncio.wait_for(async_wrap(future), timeout)
    except Exception:
        # api call failure, write to queue
        try:
            q.put((topic, data, attrs))
        except DatabaseError:
            # sqlite queue is probably out of space
            return response.text("", HTTP_STATUS.INSUFFICIENT_STORAGE)
    return response.text("")


def init_app(app: Sanic) -> Tuple[PublisherClient, SQLiteAckQueue]:
    """Initialize Sanic app with url rules."""
    # Get PubSub timeout
    timeout = app.config.get("PUBLISH_TIMEOUT_SECONDS")
    # Initialize PubSub client
    client = PublisherClient()
    # Use a SQLiteAckQueue because:
    # * we use acks to ensure messages only removed on success
    # * persist-queue's SQLite*Queue is faster than its Queue
    # * SQLite provides thread-safe and process-safe access
    queue_config = {
        key[6:].lower(): value
        for key, value in app.config.items()
        if key.startswith("QUEUE_")
    }
    q = SQLiteAckQueue(**queue_config)
    # get metadata_headers config
    metadata_headers = app.config["METADATA_HEADERS"]
    # validate attribute keys
    for attribute in metadata_headers.values():
        if len(attribute.encode("utf8")) > 256:
            # https://cloud.google.com/pubsub/quotas#resource_limits
            raise ValueError("Metadata attribute exceeds key size limit of 256 bytes")
    # generate one view_func per topic
    handlers = {
        route.topic: partial(
            submit,
            client=client,
            q=q,
            topic=route.topic,
            metadata_headers=metadata_headers,
            timeout=timeout,
        )
        for route in app.config["ROUTE_TABLE"]
    }
    # add routes for ROUTE_TABLE
    for route in app.config["ROUTE_TABLE"]:
        app.add_route(
            handler=handlers[route.topic],
            uri=route.uri,
            methods=[method.upper() for method in route.methods],
            # required because handler.__name__ does not exist
            # must be a unique name for each handler
            name="submit_" + route.topic,
        )
    return client, q
