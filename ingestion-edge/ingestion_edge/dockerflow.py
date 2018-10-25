# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Entrypoint for adding Dockerflow routes to the application.

See https://github.com/mozilla-services/Dockerflow
"""

from dockerflow import checks
from dockerflow.sanic import Dockerflow
from functools import partial
from sanic import Sanic
import os
import os.path

dockerflow = Dockerflow()

LOW_DISK_ERROR_ID = "edge.checks.E001"
NO_QUEUE_WARNING_ID = "edge.checks.W001"


def check_disk_bytes_free(app):
    """Check disk for QUEUE_PATH has minimum amount of bytes free."""
    path = app.config.get("QUEUE_PATH")
    threshold = app.config.get("MINIMUM_DISK_FREE_BYTES")

    if None in (path, threshold):
        return []

    try:
        status = os.statvfs(path)
    except FileNotFoundError:
        return [checks.Warn("queue path does not exist", NO_QUEUE_WARNING_ID)]

    bytes_free = status.f_bfree * status.f_frsize
    if bytes_free < threshold:
        return [checks.Error("disk bytes free below threshold", id=LOW_DISK_ERROR_ID)]


def init_app(app: Sanic):
    """Initialize Sanic app with dockerflow apis."""
    dockerflow.check(name="check_disk_bytes_free")(partial(check_disk_bytes_free, app))
    dockerflow.init_app(app)
