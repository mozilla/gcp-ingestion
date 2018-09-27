# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Definition of our Flask application."""

from flask import Flask
from .dockerflow import dockerflow
from .publish import publish

app = Flask(__name__)
app.register_blueprint(publish)
dockerflow.init_app(app)
