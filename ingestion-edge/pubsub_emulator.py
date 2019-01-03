# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Pubsub Emulator for testing."""

from google.cloud.pubsub_v1.proto import pubsub_pb2_grpc, pubsub_pb2
from google.protobuf import empty_pb2, json_format
from typing import Dict, Set
import concurrent.futures
import grpc
import logging
import os
import time
import uuid


class LazyFormat:
    """Container class for lazily formatting logged protobuf."""

    def __init__(self, value):
        """Initialize new container."""
        self.value = value

    def __str__(self):
        """Get str(dict(value)) without surrounding curly braces."""
        return str(json_format.MessageToDict(self.value))[1:-1]


class Subscription:
    """Container class for subscription messages."""

    def __init__(self):
        """Initialize subscription messages queue."""
        self.published = []
        self.pulled = {}


class PubsubEmulator(
    pubsub_pb2_grpc.PublisherServicer, pubsub_pb2_grpc.SubscriberServicer
):
    """Pubsub gRPC emulator for testing."""

    def __init__(
        self,
        host: str = os.environ.get("HOST", "0.0.0.0"),
        max_workers: int = int(os.environ.get("MAX_WORKERS", 1)),
        port: int = int(os.environ.get("PORT", 0)),
    ):
        """Initialize a new PubsubEmulator and add it to a gRPC server."""
        self.logger = logging.getLogger("pubsub_emulator")
        self.topics: Dict[str, Set[Subscription]] = {}
        self.subscriptions: Dict[str, Subscription] = {}
        self.status_codes: Dict[str, grpc.StatusCode] = {}
        self.sleep = None
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self.create_server()

    def create_server(self):
        """Create and start a new grpc.Server configured with PubsubEmulator."""
        self.server = grpc.server(
            concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers),
            options=[
                ("grpc.max_receive_message_length", -1),
                ("grpc.max_send_message_length", -1),
            ],
        )
        self.port = self.server.add_insecure_port("%s:%d" % (self.host, self.port))
        pubsub_pb2_grpc.add_PublisherServicer_to_server(self, self.server)
        pubsub_pb2_grpc.add_SubscriberServicer_to_server(self, self.server)
        self.server.start()
        self.logger.info(
            "Listening on %s:%d",
            self.host,
            self.port,
            extra={"host": self.host, "port": self.port},
        )

    def CreateTopic(self, request, context):  # noqa: D403
        """CreateTopic implementation."""
        self.logger.debug("CreateTopic(%s)", LazyFormat(request))
        if request.name in self.topics:
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
        else:
            self.topics[request.name] = set()
        return request

    def DeleteTopic(self, request, context):  # noqa: D403
        """DeleteTopic implementation."""
        self.logger.debug("DeleteTopic(%s)", LazyFormat(request))
        try:
            self.topics.pop(request.topic)
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
        return empty_pb2.Empty()

    def CreateSubscription(self, request, context):  # noqa: D403
        """CreateSubscription implementation."""
        self.logger.debug("CreateSubscription(%s)", LazyFormat(request))
        if request.name in self.subscriptions:
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
        elif request.topic not in self.topics:
            context.set_code(grpc.StatusCode.NOT_FOUND)
        else:
            subscription = Subscription()
            self.subscriptions[request.name] = subscription
            self.topics[request.topic].add(subscription)
        return request

    def DeleteSubscription(self, request, context):  # noqa: D403
        """DeleteSubscription implementation."""
        self.logger.debug("DeleteSubscription(%s)", LazyFormat(request))
        try:
            subscription = self.subscriptions.pop(request.subscription)
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
        else:
            for subscriptions in self.topics.values():
                subscriptions.discard(subscription)
        return empty_pb2.Empty()

    def Publish(self, request, context):
        """Publish implementation."""
        self.logger.debug("Publish(%.100s)", LazyFormat(request))
        if request.topic in self.status_codes:
            context.set_code(self.status_codes[request.topic])
            return pubsub_pb2.PublishResponse()
        if self.sleep is not None:
            time.sleep(self.sleep)
        message_ids = []
        try:
            subscriptions = self.topics[request.topic]
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
        else:
            message_ids = [uuid.uuid4().hex for _ in range(len(request.messages))]
            for _id, message in zip(message_ids, request.messages):
                message.message_id = _id
            for subscription in subscriptions:
                subscription.published.extend(request.messages)
        return pubsub_pb2.PublishResponse(message_ids=message_ids)

    def Pull(self, request, context):
        """Pull implementation."""
        self.logger.debug("Pull(%.100s)", LazyFormat(request))
        received_messages = []
        try:
            subscription = self.subscriptions[request.subscription]
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
        else:
            messages = subscription.published[: request.max_messages or 100]
            subscription.pulled.update(
                {message.message_id: message for message in messages}
            )
            for message in messages:
                try:
                    subscription.published.remove(message)
                except ValueError:
                    pass
            received_messages = [
                pubsub_pb2.ReceivedMessage(ack_id=message.message_id, message=message)
                for message in messages
            ]
        return pubsub_pb2.PullResponse(received_messages=received_messages)

    def Acknowledge(self, request, context):
        """Acknowledge implementation."""
        self.logger.debug("Acknowledge(%s)", LazyFormat(request))
        try:
            subscription = self.subscriptions[request.subscription]
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
        else:
            for ack_id in request.ack_ids:
                try:
                    subscription.pulled.pop(ack_id)
                except KeyError:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
        return empty_pb2.Empty()

    def ModifyAckDeadline(
        self,
        request: pubsub_pb2.ModifyAckDeadlineRequest,
        context: grpc.ServicerContext,
    ) -> empty_pb2.Empty:  # noqa: D403
        """ModifyAckDeadline implementation."""
        self.logger.debug("ModifyAckDeadline(%s)", LazyFormat(request))
        try:
            subscription = self.subscriptions[request.subscription]
        except KeyError:
            context.abort(grpc.StatusCode.NOT_FOUND, "Subscription not found")
        # deadline is not tracked so only handle expiration when set to 0
        if request.ack_deadline_seconds == 0:
            for ack_id in request.ack_ids:
                try:
                    # move message from pulled back to published
                    subscription.published.append(subscription.pulled.pop(ack_id))
                except KeyError:
                    context.abort(grpc.StatusCode.NOT_FOUND, "Ack ID not found")
        return empty_pb2.Empty()

    def UpdateTopic(self, request, context):
        """Repurpose UpdateTopic API for setting up test conditions.

        :param request.topic.name: Name of the topic that needs overrides.
        :param request.update_mask.paths: A list of overrides, of the form
        "key=value".

        Valid override keys are "status_code" and "sleep". An override value of
        "" disables the override.

        For the override key "status_code" the override value indicates the
        status code that should be returned with an empty response by Publish
        requests, and non-empty override values must be a property of
        `grpc.StatusCode` such as "UNIMPLEMENTED".

        For the override key "sleep" the override value indicates a number of
        seconds Publish requests should sleep before returning, and non-empty
        override values must be a valid float.
        """
        self.logger.debug("UpdateTopic(%s)", LazyFormat(request))
        try:
            for override in request.update_mask.paths:
                key, value = override.split("=", 1)
                if key.lower() in ("status_code", "statuscode"):
                    if value:
                        self.status_codes[request.topic.name] = getattr(
                            grpc.StatusCode, value.upper()
                        )
                    else:
                        del self.status_codes[request.topic.name]
                elif key.lower() == "sleep":
                    if value:
                        self.sleep = float(value)
                    else:
                        self.sleep = None
                else:
                    raise ValueError()
        except (AttributeError, KeyError, ValueError):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        return request.topic


def main():
    """Run PubsubEmulator gRPC server."""
    # configure logging
    logger = logging.getLogger("pubsub_emulator")
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
    # start server
    server = PubsubEmulator().server
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        server.stop(grace=None)


if __name__ == "__main__":
    main()
