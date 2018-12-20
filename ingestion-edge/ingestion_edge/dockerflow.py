# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Entrypoint for adding Dockerflow routes to the application.

See https://github.com/mozilla-services/Dockerflow
"""

from dockerflow import checks
from dockerflow.sanic import Dockerflow
from functools import partial
from persistqueue.sqlackqueue import SQLiteAckQueue
from sanic import Sanic
import os

LOW_DISK_ERROR_ID = "edge.checks.E001"
QUEUE_ERROR_ID = "edge.checks.E002"
NO_QUEUE_WARNING_ID = "edge.checks.W001"
QUEUE_PENDING_INFO_ID = "edge.checks.I001"
QUEUE_UNACKED_INFO_ID = "edge.checks.I002"


def check_disk_bytes_free(app: Sanic, q: SQLiteAckQueue):
    """Check disk for QUEUE_PATH has minimum amount of bytes free."""
    threshold = app.config.get("MINIMUM_DISK_FREE_BYTES")

    if None in (q, threshold) or q.path in (None, ":memory:"):
        return []

    try:
        status = os.statvfs(q.path)
    except FileNotFoundError:
        return [checks.Warning("queue path does not exist", id=NO_QUEUE_WARNING_ID)]

    bytes_free = status.f_bfree * status.f_frsize
    if bytes_free < threshold:
        return [checks.Error("disk bytes free below threshold", id=LOW_DISK_ERROR_ID)]
    else:
        return []


def check_queue_size(q: SQLiteAckQueue):
    """Check queue size."""
    try:
        if q is not None:
            if q.size > 0:
                return [
                    checks.Info(
                        "queue contains pending messages", id=QUEUE_PENDING_INFO_ID
                    )
                ]
            elif q.unack_count() > 0:
                return [
                    checks.Info(
                        "queue contains unacked messages", id=QUEUE_UNACKED_INFO_ID
                    )
                ]
    except Exception:
        return [checks.Error("queue raised exception on access", id=QUEUE_ERROR_ID)]
    else:
        return []


def init_app(app: Sanic, q: SQLiteAckQueue) -> Dockerflow:
    """Initialize Sanic app with dockerflow apis."""
    dockerflow = Dockerflow()
    dockerflow.check(name="check_disk_bytes_free")(
        partial(check_disk_bytes_free, app=app, q=q)
    )
    dockerflow.check(name="check_queue_size")(partial(check_queue_size, q=q))
    dockerflow.init_app(app)
