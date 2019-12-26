"""Instantiated Sanic app for WSGI."""

from .config import logger
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
        host, port = sock.getsockname()
        logger.info(f"Listening on {host}:{port}", extra={"host": host, "port": port})
        app.run(sock=sock)


main()
