"""A simple server that print out the event message from the run using Stream."""
import typing

from bluesky.callbacks.zmq import RemoteDispatcher
import rapidz


def simple_stream_server(
    stream: rapidz.Stream,
    outbound_proxy_address: typing.Union[str, tuple],
    prefix: bytes = b''
) -> RemoteDispatcher:
    """Create a simple server that prints everything it gets.

    Parameters
    ----------
    stream :
        The input node in a stream to process the data documents.

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
    server.subscribe(lambda *docs: stream.emit(docs))
    return server


def simple_stream():
    """Create a simple stream to test the simple stream server."""
    source = rapidz.Stream()
    rapidz.sink(source, print)
    return source
