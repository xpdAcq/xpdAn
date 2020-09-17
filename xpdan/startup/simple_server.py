"""A simple server that print out the message from the run. It is an example for writing server."""
import typing as tp

from bluesky.callbacks.zmq import RemoteDispatcher


def simple_server(
    outbound_proxy_address: tp.Union[str, tuple],
    prefix: bytes = b''
) -> RemoteDispatcher:
    """Create a simple server that prints everything it gets.

    Parameters
    ----------
    outbound_proxy_address :
        The outbound proxy address.

    prefix :
        The prefix of the message that the server is interested in.

    Returns
    -------
    server :
        A simple server. Use `server.start()` to run the server.
    """
    server = RemoteDispatcher(outbound_proxy_address, prefix=prefix)
    server.subscribe(print)
    return server
