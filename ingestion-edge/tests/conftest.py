# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from sanic.testing import SanicTestClient
from ingestion_edge.create_app import create_app
from tempfile import TemporaryDirectory
from typing import Generator
import pytest
import sys
import traceback


@pytest.fixture
def client() -> Generator[SanicTestClient, None, None]:
    with TemporaryDirectory() as tmp:
        app = create_app(QUEUE_PATH=tmp, TESTING=True)

        @app.exception(Exception)
        def handler(request, exception):
            print(traceback.format_exc(), file=sys.stderr)

        yield app.test_client
