# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from ingestion_edge import wsgi


def test_wsgi(mocker):
    runs = []
    mocker.patch.object(wsgi.app, "run", lambda **kw: runs.append(kw))
    mocker.patch.object(wsgi, "__name__", "__main__")
    wsgi.main()
    assert runs == [{"host": "0.0.0.0", "port": 8000}]
