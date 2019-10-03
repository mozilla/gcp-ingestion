
def test_wsgi(mocker):
    # late import to allow pubsub emulator configuration
    from ingestion_edge import wsgi

    socket = mocker.patch.object(wsgi, "socket")
    socket.return_value.getsockname.return_value = ("", 0)
    run = mocker.patch.object(wsgi.app, "run")
    mocker.patch.object(wsgi, "__name__", "__main__")
    mocker.patch.object(wsgi, "environ", {"HOST": "HOST", "PORT": "-1"})
    wsgi.main()
    socket.assert_called_once()
    socket.return_value.bind.assert_called_once_with(("HOST", -1))
    run.assert_called_once_with(sock=socket.return_value)
