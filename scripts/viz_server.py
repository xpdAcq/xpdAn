from pprint import pprint

from bluesky.callbacks.best_effort import BestEffortCallback
from bluesky.callbacks.zmq import RemoteDispatcher
from bluesky.utils import install_qt_kicker
# pull from local data, not needed at beamline
from xpdan.callbacks import RunRouter
from xpdconf.conf import glbl_dict
import matplotlib.pyplot as plt
plt.ion()


d = RemoteDispatcher(glbl_dict['outbount_proxy_address'])
install_qt_kicker(loop=d.loop)  # This may need to be d._loop depending on tag

# TODO: add filler here
# TODO: pull our vendorized version of BEC
rr = RunRouter([lambda x: BestEffortCallback(
    # overplot=False
),
                lambda x: lambda *y: pprint(y)])

d.subscribe(rr)

d.start()