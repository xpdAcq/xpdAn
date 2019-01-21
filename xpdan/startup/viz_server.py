import fire
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import SymLogNorm

from bluesky.utils import install_qt_kicker
from xpdan.vend.callbacks.best_effort import BestEffortCallback
from xpdan.vend.callbacks.broker import LiveImage
from xpdan.vend.callbacks.core import RunRouter
from xpdan.vend.callbacks.zmq import RemoteDispatcher
from xpdconf.conf import glbl_dict
from xpdview.callbacks import LiveWaterfall

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

    if handlers is None:
        for db in ['exp_db', 'an_db']:
            if db in glbl_dict:
                handlers = glbl_dict[db].reg.handler_reg
                break
    if prefix is None:
        prefix = [b"an", b"raw"]
    figure_pool = {}

    # TODO: maybe remove this?
    def fig_factory(x):
        """Create figures as needed, reusing old figures when they are no longer
        in use (eg the run has finished)

        Parameters
        ----------
        x : str
            The figure label name

        Returns
        -------
        fig : Figure
            The figure
        """
        # if the figure is closed remove it from the pool
        remove_figs = []
        for fig in figure_pool:
            if not plt.fignum_exists(fig.number):
                remove_figs.append(fig)
        for fig in remove_figs:
            figure_pool.pop(fig)

        for fig, in_use in figure_pool.items():
            if not in_use:
                fig.clear()
                figure_pool[fig] = True
                return fig
        fig = plt.figure(x)
        figure_pool[fig] = True
        return fig

    def teardown(fig):
        """Slate the figure for reuse

        Parameters
        ----------
        fig : Figure
            The figure to be reused

        Returns
        -------

        """
        figure_pool[fig] = False

    d = RemoteDispatcher(outbound_proxy_address, prefix=prefix)
    install_qt_kicker(loop=d.loop)

    rr = RunRouter(
        [
            lambda x: BestEffortCallback(
                # fig_factory=fig_factory, teardown=teardown,
                table_enabled=False
            ),
            lambda x: LiveWaterfall(
                "r", "gr", units=("A", "1/A**2"), window_title="PDF"
            ),
            lambda x: LiveWaterfall(
                "q",
                "mean",
                units=("1/A", "Intensity"),
                window_title="{} vs {}".format("mean", "q"),
            ),
            lambda x: LiveWaterfall(
                "q",
                "std",
                units=("1/A", "Intensity"),
                window_title="{} vs {}".format("std", "q"),
            ),
            lambda x: LiveWaterfall(
                "tth",
                "mean",
                units=("Degree", "Intensity"),
                window_title="{} vs {}".format("mean", "tth"),
            ),
            lambda x: if_correct_start(
                LiveImage(
                    handler_reg=handlers,
                    cmap="viridis",
                    norm=SymLogNorm(1),
                    limit_func=lambda x: (np.nanmin(x), np.nanmax(x)),
                ),
                x,
            ),
        ]
    )

    d.subscribe(rr)
    print("Starting Viz Server")
    d.start()


def run_main():
    fire.Fire(run_server)


if __name__ == "__main__":
    run_main()
