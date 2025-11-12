from ingestion_edge.dockerflow import init_app
from sanic.request import Request
import pytest
import json


@pytest.fixture(autouse=True)
def init(app, mocker):
    mocker.patch("ingestion_edge.dockerflow.check_disk_bytes_free", lambda app, q: [])
    mocker.patch("ingestion_edge.dockerflow.check_queue_size", lambda q: [])
    init_app(app, {})


def test_heartbeat(app):
    _, response = app.test_client.get("/__heartbeat__")
    assert response.status == 200
    assert json.loads(response.body.decode()) == {
        "status": "ok",
        "checks": {"check_disk_bytes_free": "ok", "check_queue_size": "ok"},
        "details": {},
    }


def test_lbheartbeat(app):
    _, response = app.test_client.get("/__lbheartbeat__")
    assert response.status == 200
    assert response.body == b""


def test_version(app):
    _, response = app.test_client.get("/__version__")
    assert response.status == 200
    assert set(json.loads(response.body.decode()).keys()) == {
        "build",
        "commit",
        "source",
        "version",
    }
