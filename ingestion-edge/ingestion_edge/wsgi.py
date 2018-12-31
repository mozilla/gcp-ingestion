# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

"""Instantiated Sanic app for WSGI."""

from .create_app import create_app
from os import environ
from socket import socket

app = create_app()


def main():
    """Main."""
    if __name__ == "__main__":
        host = environ.get("HOST", "0.0.0.0")
        port = int(environ.get("PORT", 8000))
        sock = socket()
        sock.bind((host, port))
        print("Listening on port %s" % sock.getsockname()[1])
        app.run(sock=sock)


main()
