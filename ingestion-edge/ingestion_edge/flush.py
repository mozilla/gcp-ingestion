"""Fallback logic for retrying queued submit requests."""

from dataclasses import dataclass
from functools import partial
from typing import Dict, Optional, Tuple

from google.cloud.pubsub_v1 import PublisherClient
from persistqueue import SQLiteAckQueue
from persistqueue.exceptions import Empty
from sanic import Sanic
import asyncio
import uvloop

from .publish import get_client, get_queue
from .config import get_config_dict, logger


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
                    future = asyncio.wrap_future(
                        self.client.publish(topic, data, **attrs)
                    )
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
            self.q.clear_acked_data(max_delete=None, keep_latest=None)
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

    async def run_until_complete(self):
        """Call flush until queue is empty."""
        while not self.q.empty():
            try:
                await self._flush()
            except Exception:
                logger.exception("pubsub unavailable")


def get_flush(config: dict, client: PublisherClient, q: SQLiteAckQueue) -> Flush:
    """Create a Flush instance."""
    return Flush(
        client,
        q,
        **{
            key[6:].lower(): value
            for key, value in config.items()
            if key.startswith("FLUSH_")
        }
    )


def init_app(app: Sanic, client: PublisherClient, q: SQLiteAckQueue):
    """Initialize Sanic app with url rules."""
    flush = get_flush(app.config, client, q)
    # schedule periodic flush in background on app start
    app.listener("before_server_start")(flush.before_server_start)
    # schedule flush on shutdown
    app.listener("after_server_stop")(flush.after_server_stop)


def main():
    """Flush until queue is empty."""
    config = get_config_dict()
    flush = get_flush(config, get_client(config), get_queue(config))
    asyncio.get_event_loop().run_until_complete(flush.run_until_complete())


if __name__ == "__main__":
    main()
