"""Sanic app configuration.

Some configuration is hard-coded here, but most is provided via
environment variables.
"""

from dataclasses import dataclass
from logging import getLogger
from logging.config import dictConfig
from os import environ
from typing import Tuple
import json

log_level = environ.get("LOG_LEVEL", "DEBUG").upper()
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
                "level": log_level,
                "class": "logging.StreamHandler",
                "formatter": "json",
            }
        },
        "loggers": {
            "request.summary": {"handlers": ["console"], "level": log_level},
            "ingestion-edge": {"handlers": ["console"], "level": log_level},
        },
    }
)
logger = getLogger("ingestion-edge")


@dataclass
class Route:
    """Dataclass for entries in ROUTE_TABLE."""

    uri: str
    topic: str
    methods: Tuple = ("POST", "PUT")


ROUTE_TABLE = [Route(*route) for route in json.loads(environ.get("ROUTE_TABLE", "[]"))]

QUEUE_PATH = environ.get("QUEUE_PATH", "queue")

MINIMUM_DISK_FREE_BYTES = int(environ.get("MINIMUM_DISK_FREE_BYTES", 0)) or None

with open("metadata_headers.json", "r") as fp:
    METADATA_HEADERS = {
        header: header.replace("-", "_")
        for raw in json.load(fp)
        for header in (raw.strip().lower(),)
        if header
    }

PUBLISH_TIMEOUT_SECONDS = float(environ.get("PUBLISH_TIMEOUT_SECONDS", 1))

FLUSH_CONCURRENT_BYTES = int(environ.get("FLUSH_CONCURRENT_BYTES", 1e7))

FLUSH_CONCURRENT_MESSAGES = int(environ.get("FLUSH_CONCURRENT_MESSAGES", 1000))

FLUSH_SLEEP_SECONDS = float(environ.get("FLUSH_SLEEP_SECONDS", 1))


def get_config_dict() -> dict:
    """Return the config values from this module as a dict."""
    return {key: value for key, value in globals().items() if key == key.upper()}
