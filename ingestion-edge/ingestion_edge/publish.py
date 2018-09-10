# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from .conf import ROUTE_TABLE
from datetime import datetime
from flask import Blueprint, request
from google.cloud.pubsub import PublisherClient
from typing import Tuple

publish = Blueprint('publish', __name__)
client = PublisherClient()


def handle_request(topic: str, **kwargs) -> Tuple[str, int]:
    # extract metadata
    attrs = dict(
        submission_timestamp=datetime.utcnow().isoformat() + "Z",
        uri=request.path,
        protocol=request.scheme,
        method=request.method,
        args=request.query_string,
        remote_addr=request.remote_addr,
        content_length=str(request.content_length),
        date=request.date,
        dnt=request.headers.get("DNT"),
        host=request.host,
        user_agent=request.headers.get("User-Agent"),
        x_forwarded_for=request.headers.get("X-Forwarded-For"),
        x_pingsender_version=request.headers.get("X-Pingsender-Version"),
    )
    # publish message
    future = client.publish(
        topic=topic,
        data=request.get_data(),
        **{
            key: value
            for key, value in attrs.items()
            if value is not None
        }
    )
    # returns message_id, which we don't use
    future.result()
    return "", 200


for path, topic, methods in ROUTE_TABLE:
    publish.route(
        path,
        defaults={'topic': topic},
        methods=methods,
    )(handle_request)
