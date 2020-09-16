import fire

from rapidz import Stream
from shed.writers import NpyWriter
from bluesky.callbacks.core import RunRouter
from bluesky.callbacks.zmq import RemoteDispatcher
from xpdconf.conf import glbl_dict


def run_server(
    data_dir,
    outbound_proxy_address=glbl_dict["outbound_proxy_address"],
    prefix=b"an",
):
    """Start up the databroker server for analyzed data.

    Parameters
    ----------
    data_dir : str
        The directory to save the array data into.
    outbound_proxy_address : str, optional
        The address and port of the zmq proxy. Defaults to
        ``glbl_dict["outbound_proxy_address"]``
    prefix : bytes or list of bytes, optional
        The Publisher channels to listen to. Defaults to
        ``b"an"``
    """

    d = RemoteDispatcher(outbound_proxy_address, prefix=prefix)
    an_broker = glbl_dict["an_db"]

    an_source = Stream()
    an_source.Store(data_dir, NpyWriter).starsink(an_broker.insert)

    rr = RunRouter(
        [
            lambda x: (lambda *nd: an_source.emit(nd))
            if x.get("analysis_stage", None) == "pdf"
            else None,
            lambda x: (lambda *nd: an_source.emit(nd))
            if x.get("analysis_stage", None) == "integration"
            else None,
        ]
    )

    d.subscribe(rr)

    print("Starting DB Server")
    d.start()


def run_main():
    fire.Fire(run_server)


if __name__ == "__main__":
    run_main()
