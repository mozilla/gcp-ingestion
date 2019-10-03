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
