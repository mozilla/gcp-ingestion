# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Entrypoint for adding Dockerflow routes to the application.

See https://github.com/mozilla-services/Dockerflow
"""

from dockerflow.sanic import Dockerflow

dockerflow = Dockerflow()
