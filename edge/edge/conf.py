# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from os import environ
from typing import List, Tuple
import json

Route = Tuple[str, str, List[str]]


def route(path: str, topic: str, methods: List[str]=["POST", "PUT"]) -> Route:
    return path, topic, methods


ROUTE_TABLE = [
    route(*value)
    for value in json.loads(environ.get("ROUTE_TABLE", "[]"))
]
