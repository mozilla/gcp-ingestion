# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Main logic for handling submit requests."""

from datetime import datetime
from flask import Flask, request
from functools import partial
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.exceptions import PublishError
from persistqueue import SQLiteAckQueue
from persistqueue.exceptions import Empty
from typing import Dict, Tuple
import google.api_core.exceptions
import json

FLUSH_EMPTY_STATUS = 204
FLUSH_COMPLETE_STATUS = 200
FLUSH_INCOMPLETE_STATUS = 504
# Errors where message should be queued locally after pubsub delivery fails
# To minimize data loss include errors that may require manual intervention
TRANSIENT_ERRORS = (
    # The API thinks the client should retry
    google.api_core.exceptions.Aborted,
    # The API was interrupted when handling the request
    google.api_core.exceptions.Cancelled,
    # The PublisherClient lacks permissions and may require manual intervention
    google.api_core.exceptions.Forbidden,
    # API call ran out of retries
    google.api_core.exceptions.RetryError,
    # API outage or connection issue
    google.api_core.exceptions.ServerError,
    # The API is throttling requests
    google.api_core.exceptions.TooManyRequests,
    # A python Future timed out
    TimeoutError,
)

client = PublisherClient()


def _publish(topic: str, data: bytes, attrs: Dict[str, str]) -> str:
    """Publish one message to the topic.

    Use PublisherClient to publish the message in a batch.

    PublishError from PublisherClient requires special handling because it is a
    non-transient error. It is raised for the whole batch and does not indicate
    what message failed or whether all messages failed. Circumvent batching to
    determine if this message in particular failed.

    Note that this method will block until it gets a result, but gevent workers
    should yield the coroutine during IO to handle multiple connections per
    worker.
    """
    try:
        return client.publish(topic, data, **attrs).result()
    except PublishError:
        # Batch failed because pubsub permanently rejected at least one message
        # retry this message alone to determine if it was rejected
        response = client.api.publish(
            topic=topic,
            messages=[{'data': data, 'attributes': attrs}],
        )
        if len(response.message_ids) != 1:
            raise PublishError("message was not successfully published")
        return response.message_ids[0]


def flush(q: SQLiteAckQueue) -> Tuple[str, int]:
    """Flush messages from the local queue to pubsub.

    Call periodically on each docker container to ensure reliable delivery.

    Access to this endpoint should be restricted.
    """
    if q.size == 0:
        # early return for empty queue
        return "", FLUSH_EMPTY_STATUS
    done = 0
    # while queue has messages ready to process
    while q.size > 0:
        try:
            message = q.get(block=False)
        except Empty:
            # queue is empty or sqlite needs a retry
            continue
        try:
            _publish(*message)
        except TRANSIENT_ERRORS:
            # message not delivered
            q.nack(message)
            # transient errors stop processing queue until next flush
            response = json.dumps(dict(done=done, pending=q.size))
            return response, FLUSH_INCOMPLETE_STATUS
        except:  # noqa: E722
            # message not delivered
            q.nack(message)
            # raise from bare except
            raise
        else:
            q.ack(message)
            done += 1
    return json.dumps(dict(done=done, pending=0)), FLUSH_COMPLETE_STATUS


def submit(q: SQLiteAckQueue, topic: str, **kwargs) -> Tuple[str, int]:
    """Deliver request to the pubsub topic.

    Deliver to the local queue to be retried on transient errors.
    """
    data = request.get_data()
    attrs = {
        key: value
        for key, value in dict(
            submission_timestamp=datetime.utcnow().isoformat() + "Z",
            uri=request.path,
            protocol=request.scheme,
            method=request.method,
            args=request.query_string.decode(),
            remote_addr=request.remote_addr,
            content_length=str(request.content_length),
            date=request.date,
            dnt=request.headers.get("DNT"),
            host=request.host,
            user_agent=request.headers.get("User-Agent"),
            x_forwarded_for=request.headers.get("X-Forwarded-For"),
            x_pingsender_version=request.headers.get("X-Pingsender-Version"),
        ).items()
        if value is not None
    }
    try:
        _publish(topic, data, attrs)
    except TRANSIENT_ERRORS:
        # transient api call failure, write to queue
        q.put((topic, data, attrs))
    return "", 200


def init_app(app: Flask):
    """Initialize Flask app with url rules."""
    # Use a SQLiteAckQueue because:
    # * we use acks to ensure messages only removed on success
    # * persist-queue's SQLite*Queue is faster than its Queue
    # * SQLite provides thread-safe and process-safe access
    q = SQLiteAckQueue(**app.config.get_namespace("QUEUE_"))
    # route flush
    app.add_url_rule("/__flush__", "flush", partial(flush, q))
    # generate one view_func per topic
    view_funcs = {
        route.topic: partial(submit, q, route.topic)
        for route in app.config["ROUTE_TABLE"]
    }
    # add routes for ROUTE_TABLE
    for route in app.config["ROUTE_TABLE"]:
        app.add_url_rule(
            route.rule,
            # required because view_func.__name__ does not exist
            # must be a unique endpoint for each view_func
            endpoint="submit_" + route.topic,
            view_func=view_funcs[route.topic],
            methods=route.methods,
        )
