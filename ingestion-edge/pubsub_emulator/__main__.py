# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Suppport `python -m pubsub_emulator`."""

from pubsub_emulator import create_server
import time


def main():
    """Run PubsubEmulator gRPC server."""
    server = create_server()[0]
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        server.stop(grace=None)


if __name__ == "__main__":
    main()
