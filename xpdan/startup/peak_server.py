from bluesky.callbacks.zmq import *
from xpdan.pipelines.to_event_model import to_event_stream_with_ind
from xpdan.vend.callbacks.core import RunRouter, StripDepVar
from xpdconf.conf import glbl_dict
from shed.simple import *
from rapidz import Stream, zip as szip
import numpy as np
from scipy.signal import peak_widths, find_peaks


def run_server(
    prefix=None,
    outbound_proxy_address=glbl_dict["outbound_proxy_address"],
    inbound_proxy_address=glbl_dict["inbound_proxy_address"],
    _publisher=None,
    x_ranges=(),
    stage="integration",
    x_name="q",
    y_name="mean",
    plot_graph=None,
):
    """Start up server for extracting single intensities

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
    x_ranges : list of float
        The windows for peak selection (``[lower_one, upper_one, lower_two,
        upper_two]``)
    stage : str
        The analysis stage to use for the data
    x_name : str
        The name of the pattern independent variable (``q`` or ``r`` for
        example)
    y_name : str
        The name of the pattern dependent variable (``mean`` or ``gr`` for
        example)
    plot_graph : None or str, optional
        If a string save a plot of the graph to that file, if None don't.
        Defaults to None
    """
    if prefix is None:
        prefix = [b"an", b"raw"]

    rd = RemoteDispatcher(outbound_proxy_address, prefix=prefix)

    if _publisher is None:
        pub = Publisher(inbound_proxy_address, prefix=b"qoi")
    else:
        pub = _publisher

    source1 = Stream()
    source2 = Stream()

    q = SimpleFromEventStream("event", ("data", x_name), upstream=source1)
    iq = SimpleFromEventStream(
        "event", ("data", y_name), upstream=source1, principle=True
    )
    windows = list(zip(x_ranges[::2], x_ranges[1::2]))

    positions = []
    peak_wdths = []
    for lower_bound, upper_bound in windows:
        y_range = iq.combine_latest(
            q.map(lambda x, y: np.argmin(np.abs(x - y)), lower_bound),
            q.map(lambda x, y: np.argmin(np.abs(x - y)), upper_bound),
            emit_on=0,
        ).starmap(lambda x, y, yy: x[y:yy])
        peak_position = (
            y_range.map(find_peaks)
            .pluck(0)
            .map(lambda x: x if len(x) == 1 else 0)
        )
        peak_width = (
            y_range.zip(peak_position)
            .starmap(peak_widths)
            .pluck(0)
            .map(lambda x: x[0] if len(x) == 1 else 0)
        )
        positions.append(peak_position)
        peak_wdths.append(peak_width)

    tes = SimpleToEventStream(
        szip(*[p.pluck(0) for p in positions], *peak_wdths),
        [f"peak_pos_{x_name}={l}-{u}" for l, u in windows]
        + [f"peak_width_{x_name}={l}-{u}" for l, u in windows],
        analysis_stage="peak_positions",
    )

    z = move_to_first(source2.starmap(StripDepVar()))
    to_event_stream_with_ind(z, tes, publisher=pub)
    if plot_graph:
        tes.visualize(plot_graph, dpi="600", ranksep=".1")

    rr = RunRouter(
        [
            lambda x: lambda *y: source2.emit(y)
            if x.get("analysis_stage", "") == "raw"
            else None,
            lambda x: lambda *y: source1.emit(y)
            if x.get("analysis_stage", "") == stage
            else None,
        ]
    )
    rd.subscribe(rr)
    print("Starting Peak Server")
    rd.start()


def run_main():
    import fire

    fire.Fire(run_server)


if __name__ == "__main__":
    run_main()
