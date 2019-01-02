# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from ingestion_edge import wsgi


def test_wsgi(mocker):
    socket = mocker.patch.object(wsgi, "socket")
    socket.return_value.getsockname.return_value = ("", 0)
    run = mocker.patch.object(wsgi.app, "run")
    mocker.patch.object(wsgi, "__name__", "__main__")
    mocker.patch.object(wsgi, "environ", {"HOST": "HOST", "PORT": "-1"})
    wsgi.main()
    socket.assert_called_once()
    socket.return_value.bind.assert_called_once_with(("HOST", -1))
    run.assert_called_once_with(sock=socket.return_value)
