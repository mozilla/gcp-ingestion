# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Pubsub Emulator for testing."""

from google.cloud.pubsub_v1.proto import pubsub_pb2_grpc, pubsub_pb2
from google.protobuf import empty_pb2
from typing import Dict, Set
import concurrent.futures
import grpc
import os
import time
import uuid


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

    def __init__(self, server: grpc.Server):
        """Initialize a new PubsubEmulator and add it to a gRPC server."""
        self.topics: Dict[str, Set[Subscription]] = {}
        self.subscriptions: Dict[str, Subscription] = {}
        self.status_codes: Dict[str, grpc.StatusCode] = {}
        self.sleep = None
        pubsub_pb2_grpc.add_PublisherServicer_to_server(self, server)
        pubsub_pb2_grpc.add_SubscriberServicer_to_server(self, server)

    def CreateTopic(self, request, context):  # noqa: D403
        """CreateTopic implementation."""
        if request.name in self.topics:
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
        else:
            self.topics[request.name] = set()
        return request

    def DeleteTopic(self, request, context):  # noqa: D403
        """DeleteTopic implementation."""
        try:
            self.topics.pop(request.topic)
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
        return empty_pb2.Empty()

    def CreateSubscription(self, request, context):  # noqa: D403
        """CreateSubscription implementation."""
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

    def UpdateTopic(self, request, context):
        """Repurpose UpdateTopic API for setting up test conditions.

        :param request.topic.name: Name of the topic that needs overrides.
        "param request.update_mask.paths: A list of overrides, over the form
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
        try:
            for override in request.update_mask.paths:
                key, value = override.split("=", 1)
                if key.lower() in ("status_code", "statuscode"):
                    if value:
                        self.status_codes[request.topic.name] = getattr(
                            grpc.StatusCode, value
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


def create_server(
    max_workers=int(os.environ.get("MAX_WORKERS", 1)),
    port=int(os.environ.get("PORT", 0)),
):
    """Create and start a new grpc.Server configured with PubsubEmulator."""
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=max_workers))
    port = server.add_insecure_port("0.0.0.0:%d" % port)
    emulator = PubsubEmulator(server)
    server.start()
    return server, port, emulator


def main():
    """Run PubsubEmulator gRPC server."""
    server = create_server()[0]
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        server.stop(grace=None)


if __name__ == "__main__":
    main()
