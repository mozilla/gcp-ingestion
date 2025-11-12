from ingestion_edge import publish
from ingestion_edge.config import Route
from sanic import Sanic
from sanic.request import Request
from unittest import mock
import pytest
import sanic.response
import asyncio


def test_missing_queue_path(app):
    app.config.update(METADATA_HEADERS={}, ROUTE_TABLE=[])
    with pytest.raises(TypeError) as e_info:
        publish.init_app(app)
    assert e_info.value.args == (
        "SQLiteAckQueue.__init__() missing 1 required positional argument: 'path'",
    )


def test_invalid_attribute_key(app):
    app.config.update(
        QUEUE_PATH=":memory:", METADATA_HEADERS={"a": "a" * 257}, ROUTE_TABLE=[]
    )
    with pytest.raises(ValueError) as e_info:
        publish.init_app(app)
    assert e_info.value.args == (
        "Metadata attribute exceeds key size limit of 256 bytes",
    )
