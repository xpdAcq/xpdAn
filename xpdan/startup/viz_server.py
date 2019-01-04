import matplotlib.pyplot as plt
from xpdan.vend.callbacks.zmq import RemoteDispatcher
from bluesky.utils import install_qt_kicker
from xpdan.vend.callbacks.best_effort import BestEffortCallback
from xpdan.vend.callbacks.broker import LiveImage

# pull from local data, not needed at beamline
from xpdan.vend.callbacks.core import RunRouter
from xpdconf.conf import glbl_dict

plt.ion()

d = RemoteDispatcher(glbl_dict["outbound_proxy_address"])
install_qt_kicker(loop=d.loop)  # This may need to be d._loop depending on tag

figure_pool = {}


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
    print(x)
    print(figure_pool)
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


# TODO: add filler here
rr = RunRouter(
    [
        lambda x: BestEffortCallback(
            fig_factory=fig_factory, table_enabled=False, teardown=teardown
        ),
        lambda x: LiveImage(cmap="viridis"),
    ]
)

d.subscribe(rr)

print("Starting Viz Server")

if __name__ == "__main__":
    d.start()
