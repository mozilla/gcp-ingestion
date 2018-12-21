# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Definition of our Sanic application."""

from sanic import Sanic
from . import config, flush, publish, dockerflow


def create_app(**kwargs) -> Sanic:
    """Generate Sanic application."""
    app = Sanic(__name__)
    app.config.from_object(config)
    app.config.update(**kwargs)
    client, q = publish.init_app(app)
    flush.init_app(app, client, q)
    dockerflow.init_app(app, q)
    return app
