"""Main logic for handling submit requests."""

from datetime import datetime
from sqlite3 import DatabaseError
from sanic import Sanic, response
from sanic.request import Request
from functools import partial
from google.api_core.retry import Retry, if_exception_type
from google.cloud.pubsub_v1 import PublisherClient
from persistqueue import SQLiteAckQueue
from typing import Callable, Dict, Optional, Tuple
from .config import logger
from .util import AsyncioBatch, HTTP_STATUS
import google.api_core.exceptions

TRANSIENT_ERRORS = if_exception_type(
    # Service initiated retry
    google.api_core.exceptions.Aborted,
    # Service interrupted when handling the request
    google.api_core.exceptions.Cancelled,
    # Service throttled the request
    google.api_core.exceptions.TooManyRequests,
    # Service outage or connection issue
    google.api_core.exceptions.ServerError,
)


def _legacy_encoding(value: Optional[str], request: Request) -> Optional[str]:
    """Convert string from UTF-8 with surrogate escape characters to ISO-8859-1.

    As of Sanic 21.3.0 in sanic.http.http1 headers are decoded with the default codec
    (UTF-8) and errors="surrogateescape", but behavior prior to that was to decode
    headers with ISO-8859-1, aka latin-1.

    Unit tests hit decode headers in sanic.http.http1, but prod and some integration
    tests decode headers in sanic.asgi with the latin-1 codec. It's not possible to
    reliably determine which codec was used from the string alone, but requests built in
    sanic.http.http1 set request.head, while those built in sanic.asgi don't set
    request.head, so it defaults to b"". Thus request.head being empty can be used as a
    proxy to determine whether legacy encoding needs to be restored.

    Surrogate escape characters cause exceptions to be thrown if they are encoded back
    to bytes without specifying an error handling strategy, such as when checking the
    byte length of individual headers, and when encoding the value as protobuf to be
    sent to PubSub.

    This changes the decoding back to ISO-8859-1 when request.head is present to
    preserve legacy behavior and ensure all character sequences are valid and safe to
    encode as UTF-8.
    """
    if value is None or request.head == b"":
        # header not present, or already using desired codec
        return value
    # Sanic used the default codec to decode value, so don't explicitly specify a codec
    # for value.encode, even though it is expected to be UTF-8
    return value.encode(errors="surrogateescape").decode("ISO-8859-1")


async def submit(
    request: Request,
    client: PublisherClient,
    q: SQLiteAckQueue,
    topic: str,
    metadata_headers: Dict[str, str],
    **kwargs,
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
                attr: _legacy_encoding(request.headers.get(header), request)
                for header, attr in metadata_headers.items()
            },
        ).items()
        if value is not None
    }
    # assert valid pubsub message
    for value in attrs.values():
        if len(value.encode("utf8")) > 1024:
            # attribute exceeds value size limit of 1024 bytes
            # https://cloud.google.com/pubsub/quotas#resource_limits
            return response.text(
                "header too large\n", HTTP_STATUS.REQUEST_HEADER_FIELDS_TOO_LARGE
            )
    try:
        await client.publish(topic, data, **attrs)
    except ValueError:
        return response.text("payload too large\n", HTTP_STATUS.PAYLOAD_TOO_LARGE)
    except Exception:
        # api call failure, write to queue
        logger.exception("pubsub unavailable")
        try:
            q.put((topic, data, attrs))
        except DatabaseError:
            logger.exception("queue full")
            # sqlite queue is probably out of space
            return response.text("", HTTP_STATUS.INSUFFICIENT_STORAGE)
    return response.text("")


def get_client(config: dict) -> PublisherClient:
    """Create a pubsub client."""
    # Initialize PubSub client
    timeout = config.get("PUBLISH_TIMEOUT_SECONDS", None)
    client = PublisherClient()
    client.api.publish = partial(
        client.api.publish,
        retry=Retry(TRANSIENT_ERRORS, deadline=timeout),
        timeout=timeout,
    )
    client._batch_class = AsyncioBatch
    return client


def get_queue(config: dict) -> SQLiteAckQueue:
    """Create a SQLiteAckQueue.

    Use a SQLiteAckQueue because:
    * we use acks to ensure messages only removed on success
    * persist-queue's SQLite*Queue is faster than its Queue
    * SQLite provides thread-safe and process-safe access
    """
    queue_config = {
        key[6:].lower(): value
        for key, value in config.items()
        if key.startswith("QUEUE_")
    }
    q: SQLiteAckQueue = SQLiteAckQueue(**queue_config, auto_resume=False)
    q.resume_unack_tasks()
    # work around https://github.com/peter-wangxu/persist-queue/pull/154
    q.total = q._count()
    return q


def init_app(app: Sanic) -> Tuple[PublisherClient, SQLiteAckQueue]:
    """Initialize Sanic app with url rules."""
    client = get_client(app.config)
    q = get_queue(app.config)
    # get metadata_headers config
    metadata_headers = app.config["METADATA_HEADERS"]
    # validate attribute keys
    for attribute in metadata_headers.values():
        if len(attribute.encode("utf8")) > 256:
            # https://cloud.google.com/pubsub/quotas#resource_limits
            raise ValueError("Metadata attribute exceeds key size limit of 256 bytes")
    # add routes for ROUTE_TABLE
    handlers: Dict[str, Callable] = {}
    for route in app.config["ROUTE_TABLE"]:
        if route.topic not in handlers:
            # generate one handler per topic
            handlers[route.topic] = partial(
                submit,
                client=client,
                q=q,
                topic=route.topic,
                metadata_headers=metadata_headers,
            )
            handlers[route.topic].__name__ = f"submit({route.topic})"
        app.add_route(
            handler=handlers[route.topic],
            uri=route.uri,
            methods=[method.upper() for method in route.methods],
        )
    return client, q
