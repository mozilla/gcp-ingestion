# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Pubsub Emulator for testing."""

from google.cloud.pubsub_v1.proto import pubsub_pb2_grpc, pubsub_pb2
from google.protobuf import empty_pb2, json_format
from typing import Dict, List, Optional, Set
import concurrent.futures
import grpc
import json
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
        topics: Optional[str] = os.environ.get("TOPICS"),
    ):
        """Initialize a new PubsubEmulator and add it to a gRPC server."""
        self.logger = logging.getLogger("pubsub_emulator")
        self.topics: Dict[str, Set[Subscription]] = {
            topic: set() for topic in (json.loads(topics) if topics else [])
        }
        self.subscriptions: Dict[str, Subscription] = {}
        self.status_codes: Dict[str, grpc.StatusCode] = {}
        self.sleep: Optional[float] = None
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

    def CreateTopic(
        self, request: pubsub_pb2.Topic, context: grpc.ServicerContext
    ):  # noqa: D403
        """CreateTopic implementation."""
        self.logger.debug("CreateTopic(%s)", LazyFormat(request))
        if request.name in self.topics:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "Topic already exists")
        self.topics[request.name] = set()
        return request

    def DeleteTopic(
        self, request: pubsub_pb2.DeleteTopicRequest, context: grpc.ServicerContext
    ):  # noqa: D403
        """DeleteTopic implementation."""
        self.logger.debug("DeleteTopic(%s)", LazyFormat(request))
        try:
            self.topics.pop(request.topic)
        except KeyError:
            context.abort(grpc.StatusCode.NOT_FOUND, "Topic not found")
        return empty_pb2.Empty()

    def CreateSubscription(
        self, request: pubsub_pb2.Subscription, context: grpc.ServicerContext
    ):  # noqa: D403
        """CreateSubscription implementation."""
        self.logger.debug("CreateSubscription(%s)", LazyFormat(request))
        if request.name in self.subscriptions:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "Subscription already exists")
        elif request.topic not in self.topics:
            context.abort(grpc.StatusCode.NOT_FOUND, "Topic not found")
        subscription = Subscription()
        self.subscriptions[request.name] = subscription
        self.topics[request.topic].add(subscription)
        return request

    def DeleteSubscription(
        self,
        request: pubsub_pb2.DeleteSubscriptionRequest,
        context: grpc.ServicerContext,
    ):  # noqa: D403
        """DeleteSubscription implementation."""
        self.logger.debug("DeleteSubscription(%s)", LazyFormat(request))
        try:
            subscription = self.subscriptions.pop(request.subscription)
        except KeyError:
            context.abort(grpc.StatusCode.NOT_FOUND, "Subscription not found")
        for subscriptions in self.topics.values():
            subscriptions.discard(subscription)
        return empty_pb2.Empty()

    def Publish(
        self, request: pubsub_pb2.PublishRequest, context: grpc.ServicerContext
    ):
        """Publish implementation."""
        self.logger.debug("Publish(%.100s)", LazyFormat(request))
        if request.topic in self.status_codes:
            context.abort(self.status_codes[request.topic], "Override")
        message_ids: List[str] = []
        try:
            subscriptions = self.topics[request.topic]
        except KeyError:
            context.abort(grpc.StatusCode.NOT_FOUND, "Topic not found")
        message_ids = [uuid.uuid4().hex for _ in request.messages]
        if self.sleep is not None:
            time.sleep(self.sleep)
            # return a valid response without recording messages
            return pubsub_pb2.PublishResponse(message_ids=message_ids)
        for _id, message in zip(message_ids, request.messages):
            message.message_id = _id
        for subscription in subscriptions:
            subscription.published.extend(request.messages)
        return pubsub_pb2.PublishResponse(message_ids=message_ids)

    def Pull(self, request: pubsub_pb2.PullRequest, context: grpc.ServicerContext):
        """Pull implementation."""
        self.logger.debug("Pull(%.100s)", LazyFormat(request))
        received_messages: List[pubsub_pb2.ReceivedMessage] = []
        try:
            subscription = self.subscriptions[request.subscription]
        except KeyError:
            context.abort(grpc.StatusCode.NOT_FOUND, "Subscription not found")
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

    def Acknowledge(
        self, request: pubsub_pb2.AcknowledgeRequest, context: grpc.ServicerContext
    ):
        """Acknowledge implementation."""
        self.logger.debug("Acknowledge(%s)", LazyFormat(request))
        try:
            subscription = self.subscriptions[request.subscription]
        except KeyError:
            context.abort(grpc.StatusCode.NOT_FOUND, "Subscription not found")
        for ack_id in request.ack_ids:
            try:
                subscription.pulled.pop(ack_id)
            except KeyError:
                context.abort(grpc.StatusCode.NOT_FOUND, "Ack ID not found")
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

    def UpdateTopic(
        self, request: pubsub_pb2.UpdateTopicRequest, context: grpc.ServicerContext
    ):
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
        override values must be a valid float. Publish requests will return
        a valid response without recording messages.
        """
        self.logger.debug("UpdateTopic(%s)", LazyFormat(request))
        for override in request.update_mask.paths:
            key, value = override.split("=", 1)
            if key.lower() in ("status_code", "statuscode"):
                if value:
                    try:
                        self.status_codes[request.topic.name] = getattr(
                            grpc.StatusCode, value.upper()
                        )
                    except AttributeError:
                        context.abort(
                            grpc.StatusCode.INVALID_ARGUMENT, "Invalid status code"
                        )
                else:
                    try:
                        del self.status_codes[request.topic.name]
                    except KeyError:
                        context.abort(
                            grpc.StatusCode.NOT_FOUND, "Status code override not found"
                        )
            elif key.lower() == "sleep":
                if value:
                    try:
                        self.sleep = float(value)
                    except ValueError:
                        context.abort(
                            grpc.StatusCode.INVALID_ARGUMENT, "Invalid sleep time"
                        )
                else:
                    self.sleep = None
            else:
                context.abort(grpc.StatusCode.Not_FOUND, "Path not found")
        return request.topic


def main():
    """Run PubsubEmulator gRPC server."""
    # configure logging
    logger = logging.getLogger("pubsub_emulator")
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "DEBUG").upper()))
    # start server
    server = PubsubEmulator().server
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        server.stop(grace=None)


if __name__ == "__main__":
    main()
