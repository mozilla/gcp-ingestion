# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from ingestion_edge.create_app import create_app
from ingestion_edge.config import Route
from sanic.request import Request
import pytest


class Config:
    METADATA_HEADERS = {"Meta-Data": "headers"}

    ROUTE_TABLE = [
        Route("/stub/<suffix:path>", "stub_installer", ("GET",)),
        Route("/submit/telemetry/<suffix:path>", "telemetry"),
        Route("/submit/sslreports", "tls_error_reports", ("post", "put")),
        Route("/submit/<suffix:path>", "ingestion", ("PoSt", "pUt")),
        Route("/submit", "ingestion"),
    ]


def generate_expect(uri_bytes, api_call="submit", method="GET", **kwargs):
    return (api_call, Request(uri_bytes, {}, "1.1", method, "http"), [], kwargs)


submit_kwargs = {
    "q": {"path": "queue_path"},
    "metadata_headers": Config.METADATA_HEADERS,
}


@pytest.mark.parametrize(
    "expect",
    [
        generate_expect(b"/__flush__", "flush", q={"path": "queue_path"}),
        generate_expect(
            b"/stub/some/path/with=stuff",
            topic="stub_installer",
            suffix="some/path/with=stuff",
            **submit_kwargs
        ),
        generate_expect(
            b"/submit/sslreports",
            method="PUT",
            topic="tls_error_reports",
            **submit_kwargs
        ),
        generate_expect(
            b"/submit/telemetry/some/path/with=stuff",
            method="POST",
            topic="telemetry",
            suffix="some/path/with=stuff",
            **submit_kwargs
        ),
        generate_expect(
            b"/submit/some/path/with=stuff",
            method="POST",
            topic="ingestion",
            suffix="some/path/with=stuff",
            **submit_kwargs
        ),
        generate_expect(b"/submit", method="PUT", topic="ingestion", **submit_kwargs),
    ],
)
async def test_endpoint(mocker, expect):
    responses = []

    mocker.patch("ingestion_edge.publish.SQLiteAckQueue", dict)
    mocker.patch("ingestion_edge.publish.PublisherClient", list)
    mocker.patch(
        "ingestion_edge.publish.flush",
        lambda req, client, **kw: ("flush", req, client, kw),
    )
    mocker.patch(
        "ingestion_edge.publish.submit",
        lambda req, client, **kw: ("submit", req, client, kw),
    )
    mocker.patch("ingestion_edge.create_app.config", Config)
    app = create_app(QUEUE_PATH="queue_path")
    await app.handle_request(expect[1], lambda r: responses.append(r), None)

    assert responses == [expect]
