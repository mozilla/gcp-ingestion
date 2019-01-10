# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from ingestion_edge import publish
from ingestion_edge.config import Route
from sanic import Sanic
from sanic.request import Request
import pytest

ROUTE_TABLE = [
    Route("/stub/<suffix:path>", "stub_installer", ("GET",)),
    Route("/submit/telemetry/<suffix:path>", "telemetry"),
    Route("/submit/sslreports", "tls_error_reports", ("post", "put")),
    Route("/submit/<suffix:path>", "ingestion", ("PoSt", "pUt")),
    Route("/submit", "ingestion"),
]


@pytest.fixture
def app():
    app = Sanic()
    app.config.update(QUEUE_PATH=":memory:", METADATA_HEADERS={}, ROUTE_TABLE=[])
    return app


@pytest.mark.parametrize(
    "uri_bytes,method,kwargs",
    [
        (
            b"/stub/some/path/with=stuff",
            "GET",
            {"topic": "stub_installer", "suffix": "some/path/with=stuff"},
        ),
        (
            b"/submit/telemetry/path/with=stuff",
            "POST",
            {"topic": "telemetry", "suffix": "path/with=stuff"},
        ),
        (
            b"/submit/some/path/with=stuff",
            "PUT",
            {"topic": "ingestion", "suffix": "some/path/with=stuff"},
        ),
        (b"/submit/sslreports", "POST", {"topic": "tls_error_reports"}),
        (b"/submit", "PUT", {"topic": "ingestion"}),
    ],
)
async def test_endpoint(app, kwargs, method, mocker, uri_bytes):
    mocker.patch("ingestion_edge.publish.SQLiteAckQueue", dict)
    client = mocker.patch("ingestion_edge.publish.PublisherClient").return_value
    mocker.patch("ingestion_edge.publish.submit", lambda _, **kw: kw)
    app.config["ROUTE_TABLE"] = ROUTE_TABLE
    publish.init_app(app)
    responses = []
    request = Request(uri_bytes, {}, "1.1", method, None)
    await app.handle_request(request, lambda r: responses.append(r), None)
    assert responses == [
        dict(client=client, q={"path": ":memory:"}, metadata_headers={}, **kwargs)
    ]


def test_missing_queue_path(app):
    del app.config["QUEUE_PATH"]
    with pytest.raises(TypeError) as e_info:
        publish.init_app(app)
    assert e_info.value.args == (
        "__init__() missing 1 required positional argument: 'path'",
    )


def test_invalid_attribute_key(app):
    app.config["METADATA_HEADERS"] = {"a": "a" * 257}
    with pytest.raises(ValueError) as e_info:
        publish.init_app(app)
    assert e_info.value.args == (
        "Metadata attribute exceeds key size limit of 256 bytes",
    )
