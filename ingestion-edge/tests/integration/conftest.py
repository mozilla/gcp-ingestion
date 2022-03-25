import os
import subprocess
import sys
from shutil import which
from time import sleep
from typing import Iterator, Optional, Tuple

# importing from private module _pytest for types only
import _pytest.config.argparsing
import _pytest.fixtures
import psutil
import pytest
import requests
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from google.cloud.pubsub_v1.types import PublisherOptions
from google.api_core.retry import Retry


def pytest_addoption(parser: _pytest.config.argparsing.Parser):
    parser.addoption(
        "--server", dest="server", default=None, help="Server to run tests against"
    )
    parser.addoption(
        "--uses-cluster",
        action="store_true",
        dest="uses_cluster",
        default=False,
        help="Indicate --server has more than one process",
    )
    parser.addoption(
        "--no-verify",
        action="store_false",
        dest="verify",
        default=True,
        help="Don't verify SSL certs",
    )


@pytest.fixture(scope="session")
def pubsub(request: _pytest.fixtures.SubRequest) -> Iterator[str]:
    if "PUBSUB_EMULATOR_HOST" in os.environ:
        yield "remote"
    elif request.config.getoption("server") is None and which("gcloud"):
        os.environ["PUBSUB_EMULATOR_HOST"] = "0.0.0.0:8085"
        process = subprocess.Popen(["gcloud", "beta", "emulators", "pubsub", "start"])
        try:
            assert process.poll() is None  # emulator still running
            os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"
            yield "local"
        finally:
            del os.environ["PUBSUB_EMULATOR_HOST"]
            try:
                # allow one second for graceful termination
                process.terminate()
                process.wait(1)
            except subprocess.TimeoutExpired:
                # kill after one second
                process.kill()
                process.wait()
    else:
        yield "google"


@pytest.fixture
def publisher(pubsub: str) -> PublisherClient:
    timeout = 1
    return PublisherClient(
        publisher_options=PublisherOptions(
            retry=Retry(deadline=timeout), timeout=timeout
        )
    )


@pytest.fixture
def subscriber(pubsub: str) -> SubscriberClient:
    return SubscriberClient()


@pytest.fixture(scope="function" if os.environ.get("PORT", "0") == "0" else "session")
def server_and_process(
    pubsub: str, request: _pytest.fixtures.SubRequest
) -> Iterator[Tuple[str, Optional[subprocess.Popen]]]:
    _server = request.config.getoption("server")
    if _server is None:
        if "PORT" not in os.environ:
            os.environ["PORT"] = "0"
        process = subprocess.Popen([sys.executable, "-u", "-m", "ingestion_edge.wsgi"])
        try:
            while process.poll() is None:
                ports = [
                    conn.laddr.port
                    for conn in psutil.Process(process.pid).connections()
                ]
                if ports:
                    break
                sleep(0.1)
            assert process.poll() is None  # server still running
            yield "http://localhost:%d" % ports.pop(), process
        finally:
            try:
                # allow one second for graceful termination
                process.terminate()
                process.wait(1)
            except subprocess.TimeoutExpired:
                # kill after one second
                process.kill()
                process.wait()
    else:
        yield _server, None


@pytest.fixture
def server(server_and_process: Tuple[str, Optional[subprocess.Popen]]) -> str:
    return server_and_process[0]


@pytest.fixture
def server_process(
    server_and_process: Tuple[str, Optional[subprocess.Popen]]
) -> Optional[subprocess.Popen]:
    return server_and_process[1]


@pytest.fixture
def requests_session(request: _pytest.fixtures.SubRequest) -> requests.Session:
    session = requests.Session()
    session.verify = request.config.getoption("verify")
    return session


@pytest.fixture
def uses_cluster(request: _pytest.fixtures.SubRequest) -> bool:
    return request.config.getoption("uses_cluster")
