import fire
import matplotlib.pyplot as plt
import numpy as np
from bluesky.utils import install_qt_kicker
from matplotlib.colors import SymLogNorm
from xpdan.vend.callbacks.best_effort import BestEffortCallback
from xpdan.vend.callbacks.broker import LiveImage
from xpdan.vend.callbacks.core import RunRouter
from xpdan.vend.callbacks.zmq import RemoteDispatcher
from xpdconf.conf import glbl_dict
from xpdview.callbacks import LiveWaterfall

try:
    from xpdan.mayavi_callbacks import Live3DView
except ImportError:
    Live3DView = None


plt.ion()

black_list = ["mask"]


def if_correct_start(callback, start_doc):
    if start_doc.get("analysis_stage", "") not in black_list:
        return callback


def run_server(
    handlers=None,
    prefix=None,
    outbound_proxy_address=glbl_dict["outbound_proxy_address"],
):
    """Start up the visualization server

    Parameters
    ----------
    handlers : dict
        The map between handler specs and handler classes, defaults to
        the map used by the experimental databroker if possible
    prefix : bytes or list of bytes, optional
        The Publisher channels to listen to. Defaults to
        ``[b"an", b"raw"]``
    outbound_proxy_address : str, optional
        The address and port of the zmq proxy. Defaults to
        ``glbl_dict["outbound_proxy_address"]``
    """

    if handlers is None:
        for db in ["exp_db", "an_db"]:
            if db in glbl_dict:
                handlers = glbl_dict[db].reg.handler_reg
                break

    d = RemoteDispatcher(outbound_proxy_address, prefix=prefix)
    install_qt_kicker(loop=d.loop)

    func_l = [
        lambda x: if_correct_start(
            LiveImage(
                handler_reg=handlers,
                cmap="viridis",
                norm=SymLogNorm(1),
                limit_func=lambda x: (np.nanmin(x), np.nanmax(x)),
            ),
            x,
        ),
        lambda x: LiveWaterfall(),
    ]
    if Live3DView:
        func_l.append(lambda x: Live3DView() if 'tomo' in x['analysis_stage'] else None)
    func_l.append(
        lambda x: BestEffortCallback(table_enabled=False, overplot=False)
    )
    rr = RunRouter(func_l)

    d.subscribe(rr)
    print("Starting Viz Server")
    d.start()


def run_main():
    fire.Fire(run_server)


if __name__ == "__main__":
    run_main()
