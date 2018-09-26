# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from flask.testing import FlaskClient
from ingestion_edge.create_app import create_app
from tempfile import TemporaryDirectory
from typing import Generator
import pytest


@pytest.fixture
def client() -> Generator[FlaskClient, None, None]:
    with TemporaryDirectory() as tmp:
        app = create_app(QUEUE_PATH=tmp, TESTING=True)
        with app.test_client() as client:
            yield client
