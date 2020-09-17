import fire
from event_model import RunRouter
from rapidz import move_to_first, Stream
from rapidz.link import link
from shed import SimpleToEventStream
from xpdconf.conf import glbl_dict
from xpdtools.pipelines.qoi import amorphsivity_pipeline

from bluesky.callbacks.zmq import RemoteDispatcher, Publisher
from bluesky.utils import install_qt_kicker
from xpdan.pipelines.qoi import amorphsivity_fem, amorphsivity_tem
from xpdan.pipelines.to_event_model import to_event_stream_with_ind
from xpdan.vend.callbacks.core import StripDepVar


def run_server(
    prefix=None,
    outbound_proxy_address=glbl_dict["outbound_proxy_address"],
    inbound_proxy_address=glbl_dict["inbound_proxy_address"],
    _publisher=None,
    **kwargs
):
    """Start up the QOI server

    Parameters
    ----------
    prefix : bytes or list of bytes, optional
        The Publisher channels to listen to. Defaults to
        ``[b"an", b"raw"]``
    outbound_proxy_address : str, optional
        The address and port of the zmq proxy. Defaults to
        ``glbl_dict["outbound_proxy_address"]``
    inbound_proxy_address : str, optional
        The inbound ip address for the ZMQ server. Defaults to the value
        from the global dict
    """
    if prefix is None:
        prefix = [b"an", b"raw"]

    d = RemoteDispatcher(outbound_proxy_address, prefix=prefix)
    install_qt_kicker(loop=d.loop)

    if _publisher is None:
        an_with_ind_pub = Publisher(inbound_proxy_address, prefix=b"qoi")
    else:
        an_with_ind_pub = _publisher

    raw_source = Stream()

    # create amorphous pipeline
    amorphous_ns = link(
        *[amorphsivity_fem, amorphsivity_pipeline, amorphsivity_tem],
        source=Stream(),
        **kwargs
    )
    # Combine the data outputs with the raw independent data
    amorphous_ns.update(
        to_event_stream_with_ind(
            move_to_first(raw_source.starmap(StripDepVar())),
            *[
                node
                for node in amorphous_ns.values()
                if isinstance(node, SimpleToEventStream)
            ],
            publisher=an_with_ind_pub
        )
    )

    rr = RunRouter(
        [
            lambda x: lambda *y: raw_source.emit(y)
            if x["analysis_stage"] == "raw"
            else None,
            lambda x: lambda *y: amorphous_ns["source"].emit(y)
            if x["analysis_stage"] == "pdf"
            else None,
        ]
    )
    d.subscribe(rr)
    print("Starting QOI Server")
    d.start()


def run_main():
    fire.Fire(run_server)


if __name__ == "__main__":
    run_main()
