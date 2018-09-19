# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from flask import Flask
from .dockerflow import dockerflow
from .publish import publish
from .disk_queue import disk_queue

app = Flask(__name__)
app.register_blueprint(publish)
app.register_blueprint(disk_queue)
dockerflow.init_app(app)
