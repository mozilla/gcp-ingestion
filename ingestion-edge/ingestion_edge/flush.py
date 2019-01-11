# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Fallback logic for retrying queued submit requests."""

from dataclasses import dataclass
from functools import partial
from google.cloud.pubsub_v1 import PublisherClient
from persistqueue import SQLiteAckQueue
from persistqueue.exceptions import Empty
from sanic import Sanic
from typing import Dict, Optional, Tuple
import asyncio
import uvloop


@dataclass
class Flush:
    """Class to store state for background flush task."""

    client: PublisherClient
    q: SQLiteAckQueue
    concurrent_bytes: int
    concurrent_messages: int
    sleep_seconds: float
    running: bool = False
    task: Optional[asyncio.Task] = None
    sleep_task: Optional[asyncio.Task] = None

    def set_status(
        self, message: Tuple[str, bytes, Dict[str, str]], future: asyncio.Future
    ):
        """Set message status from future."""
        try:
            # detect exception
            future.result()
        except:  # noqa: E722
            # message was not delivered
            self.q.nack(message)
            # do not raise in callback
        else:
            # message delivered
            self.q.ack(message)

    async def _flush(self):
        """Read messages from self.q and pass them to client.publish for batching.

        Wait for all pending messages when an exception is thrown and every time
        self.concurrent_messages or self.concurrent_bytes is reached or exceeded.

        Clear acked data each time pending messages are awaited.
        """
        pending, total_bytes = [], 0
        try:
            # send one batch to client
            for _ in range(self.concurrent_messages):
                if total_bytes >= self.concurrent_bytes:
                    # batch complete
                    break
                try:
                    # get next message
                    message = self.q.get(block=False)
                except Empty:
                    # batch complete
                    break
                try:
                    # extract values
                    topic, data, attrs = message
                    # record size of message
                    total_bytes += len(data)
                    # publish message
                    future = self.client.publish(topic, data, **attrs)
                    # ack or nack by callback
                    future.add_done_callback(partial(self.set_status, message))
                    # wait for this later
                    pending.append(future)
                except Exception:
                    # don't leave message unacked in q
                    self.q.nack(message)
                    raise
            # wait for pending operations and raise on first exception
            await asyncio.gather(*pending)
        except:  # noqa: E722
            # wait for pending operations but don't raise exceptions
            await asyncio.gather(*pending, return_exceptions=True)
            raise  # from bare except
        else:
            # clean up disk space
            self.q.clear_acked_data()
            # return number of messages flushed
            return len(pending)

    async def run_forever(self):
        """Periodically call flush.

        Sleep for sleep_seconds every time the queue is empty or an exception
        is thrown.

        Must not exit unless self.running is False or an interrupt is received.
        """
        self.running = True
        while self.running:
            try:
                # flush until the queue is empty or a publish fails
                while self.running:
                    if not await self._flush():
                        break
                # wait between flushes
                self.sleep_task = asyncio.create_task(asyncio.sleep(self.sleep_seconds))
                await self.sleep_task
            except Exception:
                pass  # ignore exceptions

    async def before_server_start(self, _, loop: uvloop.Loop):
        """Execute self.run_forever() in the background."""
        self.task = loop.create_task(self.run_forever())

    async def after_server_stop(self, *_):
        """Call flush one last time after server stop."""
        # prevent further flushing
        self.running = False
        # cancel running flush
        if self.sleep_task is not None:
            self.sleep_task.cancel()
        # wait for current flush to finish
        await self.task
        # flush until empty
        while await self._flush():
            pass


def init_app(app: Sanic, client: PublisherClient, q: SQLiteAckQueue):
    """Initialize Sanic app with url rules."""
    # configure Flush instance
    flush = Flush(
        client,
        q,
        **{
            key[6:].lower(): value
            for key, value in app.config.items()
            if key.startswith("FLUSH_")
        }
    )
    # schedule periodic flush in background on app start
    app.listener("before_server_start")(flush.before_server_start)
    # schedule flush on shutdown
    app.listener("after_server_stop")(flush.after_server_stop)
