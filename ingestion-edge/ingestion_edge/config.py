# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Sanic app configuration.

Some configuration is hard-coded here, but most is provided via
environment variables.
"""

from dataclasses import dataclass
from logging.config import dictConfig
from os import environ
from typing import Tuple
import json

dictConfig(
    {
        "version": 1,
        "formatters": {
            "json": {
                "()": "dockerflow.logging.JsonLogFormatter",
                "logger_name": "ingestion-edge",
            }
        },
        "handlers": {
            "console": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "formatter": "json",
            }
        },
        "loggers": {"request.summary": {"handlers": ["console"], "level": "DEBUG"}},
    }
)


@dataclass
class Route:
    """Dataclass for entries in ROUTE_TABLE."""

    uri: str
    topic: str
    methods: Tuple = ("POST", "PUT")


ROUTE_TABLE = [Route(*route) for route in json.loads(environ.get("ROUTE_TABLE", "[]"))]

QUEUE_PATH = environ.get("QUEUE_PATH", "queue")

MINIMUM_DISK_FREE_BYTES = int(environ.get("MINIMUM_DISK_FREE_BYTES", 0)) or None

DEFAULT_METADATA_HEADERS = [
    "Content-Length",
    "Date",
    "DNT",
    "User-Agent",
    "X-Forwarded-For",
    "X-Pingsender-Version",
    "X-Pipeline-Proxy",
]

METADATA_HEADERS = {
    header: header.replace("-", "_")
    for raw in environ.get(
        "METADATA_HEADERS", ", ".join(DEFAULT_METADATA_HEADERS)
    ).split(",")
    for header in (raw.strip().lower(),)
    if header
}
