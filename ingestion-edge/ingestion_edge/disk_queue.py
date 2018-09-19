# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from .conf import QUEUE_FILE
from .conf import QUEUE_POSITION_FILE
from flask import Blueprint
from google.cloud.pubsub import PublisherClient
from os import remove
from os.path import exists
from threading import Lock
from typing import Tuple
import ujson

disk_queue = Blueprint("disk_queue", __name__)
client = PublisherClient()
read_lock = Lock()
write_lock = Lock()


def write(**kwargs):
    with write_lock:
        # TODO disable autoscaling
        with open(QUEUE_FILE, "a") as q:
            q.write(ujson.dumps(kwargs) + "\n")
            q.flush()


@disk_queue.route("/__flush__")
def flush_queue() -> Tuple[str, int]:
    if not exists(QUEUE_FILE):
        # early return for empty queue
        return "", 204
    with read_lock:
        # initialize queue state
        done = False
        start = 0
        end = 0
        if exists(QUEUE_POSITION_FILE):
            with open(QUEUE_POSITION_FILE, "r") as qp:
                start = int(qp.read())
        try:
            # process the queue
            # TODO release write lock between line reads
            with write_lock:
                # TODO rotate queue file and handle rotated files first
                with open(QUEUE_FILE, "r") as q:
                    for line in q:
                        if end < start:
                            # TODO use q.seek(start)
                            end += 1
                            continue
                        client.publish(**ujson.loads(line)).result()
                        # update end only on success
                        end += 1
            remove(QUEUE_FILE)
            done = True
        finally:
            # update the queue position
            if end > start and not done:
                # record queue position if we moved forward and didn't finish
                with open(QUEUE_POSITION_FILE, "r+") as qp:
                    qp.write(str(end))
                    qp.flush()
            elif done and start > 0:
                # remove existing queue position because we finished
                remove(QUEUE_POSITION_FILE)
    # TODO enable autoscaling
    # return number of queue requests successfully published
    return str(end - start) + "\n", 200
