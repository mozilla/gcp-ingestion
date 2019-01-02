# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Utilities."""

import asyncio


def async_wrap(future, loop=None):
    """Wrap a threading-like future in an async future that can be awaited.

    example:

    async def func():
        future = ...
        await wrap(future)
    """
    async_future = asyncio.Future()

    def result():
        try:
            async_future.set_result(future.result())
        except Exception as e:
            async_future.set_exception(e)

    if loop is None:
        loop = asyncio.get_event_loop()

    def callback(_):
        loop.call_soon_threadsafe(result)

    future.add_done_callback(callback)

    return async_future


class HTTP_STATUS:
    """HTTP Status Codes for responses."""

    OK = 200
    BAD_REQUEST = 400
    REQUEST_HEADER_FIELDS_TOO_LARGE = 431
    INSUFFICIENT_STORAGE = 507
