"""Functions to run the server."""
from bluesky.callbacks.zmq import RemoteDispatcher


def run_server(server: RemoteDispatcher, name: str = "server"):
    try:
        print(" ".join(["Start", name, "..."]))
        server.start()
    except KeyboardInterrupt:
        print(" ".join(["Shut", "down", name, "..."]))
    return
