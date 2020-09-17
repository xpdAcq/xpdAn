"""Functions to run the server."""
from bluesky.callbacks.zmq import RemoteDispatcher


def run_server(server: RemoteDispatcher, name: str = "server"):
    """Run the server.

    Parameters
    ----------
    server :
        The server.

    name :
        The name of the server to show in the log.
    """
    try:
        print(" ".join(["Start", name, "..."]))
        server.start()
    except KeyboardInterrupt:
        print(" ".join(["Shut", "down", name, "..."]))
    return
