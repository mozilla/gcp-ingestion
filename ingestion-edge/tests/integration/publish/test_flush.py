from typing import Any
import json
import os
import subprocess
import sys

from google.cloud.pubsub_v1 import PublisherClient

# importing from private module _pytest for types only
import _pytest.fixtures
import _pytest.tmpdir
import pytest

from .helpers import IntegrationTest


@pytest.fixture(scope="module", autouse=True)
def queue_dir(
    request: _pytest.fixtures.SubRequest, tmpdir_factory: _pytest.tmpdir.TempdirFactory
):
    if request.config.getoption("server") is not None:
        # need to be able to set QUEUE_PATH for only tests in this module,
        # which is incompatible with a pre-existing --server used for all tests
        pytest.skip("incompatible with --server")
    os.environ["QUEUE_PATH"] = str(tmpdir_factory.mktemp("queue"))


def test_flush(
    integration_test: IntegrationTest,
    publisher: PublisherClient,
    pubsub: Any,
    topic: str,
    server_process: subprocess.Popen,
):
    if pubsub == "google":
        pytest.skip("requires pubsub emulator")

    # override pubsub status
    publisher.update_topic({"name": topic}, {"paths": ["status_code=internal"]})
    integration_test.assert_accepted_and_queued()

    # stop server
    server_process.kill()
    server_process.wait()

    # start flush
    process = subprocess.Popen(
        [sys.executable, "-u", "-m", "ingestion_edge.flush"], stderr=subprocess.PIPE
    )
    for line in process.stderr:
        break  # wait for an error to be logged
    assert process.poll() is None  # server still running

    # restore pubsub status
    publisher.update_topic({"name": topic}, {"paths": ["status_code="]})
    try:
        process.wait(5)
    except subprocess.TimeoutExpired:
        # kill after 5 seconds
        process.kill()
    assert process.wait() == 0

    assert json.loads(line)["Fields"]["msg"] == "pubsub unavailable"
    integration_test.assert_delivered()
