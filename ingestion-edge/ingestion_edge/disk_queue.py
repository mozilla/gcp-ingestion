# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from .conf import QUEUE_FILE
from .conf import QUEUE_POSITION_FILE
from flask import Blueprint
from google.cloud.pubsub import PublisherClient
from typing import Tuple
import ujson
import threading
import os
import os.path
import sys

disk_queue = Blueprint("disk_queue", __name__)
client = PublisherClient()
read_lock = threading.Lock()
write_lock = threading.Lock()


def _remove_if_found(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def write(publish_kwargs):
    # write lock allows one writer at a time to avoid splicing writes
    with write_lock:
        # TODO disable autoscaling
        # TODO keep file pointer open and handle file rotation
        with open(QUEUE_FILE, "a") as q:
            ujson.dump(publish_kwargs, q)
            q.write("\n")
            q.flush()


@disk_queue.route("/__flush__")
def flush_queue() -> Tuple[str, int]:
    published = 0
    # read_lock allows one flush at a time to avoid duplicate publish calls
    with read_lock:
        # initialize queue state
        try:
            with open(QUEUE_POSITION_FILE, "rb") as qp:
                start = int.from_bytes(qp.read(), sys.byteorder)
        except FileNotFoundError:
            start = 0
        done = False
        end = start
        try:
            # process queue
            # TODO rotate queue file and handle rotated files first
            # NOTE rotated files will not require write_lock
            with open(QUEUE_FILE, "r") as q:
                # seek to previous end position
                q.seek(start)
                # can't use `for line in q` because it disables `q.tell()`
                # and doesn't allow for releasing write_lock between line reads
                # or using formats with framing other than newline
                while True:
                    # write_lock ensures we don't read a partial line
                    # or write to queue immediately before removing it
                    with write_lock:
                        line = q.readline()
                        if not line:
                            # at EOF, remove queue and break
                            # ignore FileNotFoundError
                            # remove position first to prevent corrupted state
                            _remove_if_found(QUEUE_POSITION_FILE)
                            _remove_if_found(QUEUE_FILE)
                            done = True
                            break
                    # when this throws an exception, stop flushing
                    # TODO return 202 for transient exceptions
                    client.publish(**ujson.loads(line)).result()
                    # record new file position
                    end = q.tell()
                    # increment counter for items successfully published
                    published += 1
        except FileNotFoundError:
            # no queue to flush
            return "", 204
        finally:
            # update queue position if we didn't finish but made progress
            if not done and end > start:
                # use upside down floor division for ceiling division
                end_length = -(-end.bit_length() // 8)
                end_bytes = end.to_bytes(end_length, sys.byteorder)
                # record queue position if we moved forward and didn't finish
                with open(QUEUE_POSITION_FILE, "wb") as qp:
                    qp.write(end_bytes)
                    qp.flush()
    # TODO enable autoscaling
    # return number of queue items successfully published
    return str(published) + "\n", 200
