import os
from tempfile import TemporaryDirectory

import tzlocal
from shed.event_streams import istar

from bluesky.callbacks.zmq import RemoteDispatcher
from databroker.assets.handlers import AreaDetectorTiffHandler
# pull from local data, not needed at beamline
from databroker.assets.sqlite import RegistryRO
from databroker.broker import Broker
from databroker.headersource.sqlite import MDSRO
from xpdan.pipelines.main import conf_main_pipeline
import matplotlib.pyplot as plt
from bluesky.callbacks.broker import LiveImage
import numpy as np
import zmq.asyncio as zmq_asyncio
from bluesky.utils import install_qt_kicker

# from xpdan.tools import better_mask_img

d = {'directory': '/home/christopher/live_demo_data',
     'timezone': tzlocal.get_localzone().zone,
     'dbpath': os.path.join('/home/christopher/live_demo_data', 'filestore')}
mds = MDSRO(d)
fs = RegistryRO(d)
fs.register_handler('AD_TIFF', AreaDetectorTiffHandler)
db = Broker(mds=mds, reg=fs)
td = TemporaryDirectory()
source = conf_main_pipeline(db, td.name,
                            # vis=False,
                            write_to_disk=False,
                            # mask_setting=None
                            )

# a = LiveImage('pe1_image')
loop = zmq_asyncio.ZMQEventLoop()
install_qt_kicker(loop=loop)

def put_in_queue(nd):
    if nd[0] == 'event':
        nd[1]['data']['pe1_image'] = np.asarray(nd[1]['data']['pe1_image'])
    # if nd[0] == 'event':
    #     db.fill_event(nd[1])
    # print(nd)
    # source.emit(nd)
    a(*nd)
    plt.pause(.1)


disp = RemoteDispatcher('127.0.0.1:5568', loop=loop)
# disp.subscribe(istar(put_in_queue))
# disp.subscribe(a)
disp.subscribe(istar(source.emit))
print("REMOTE IS READY TO START")
# disp._loop.call_later(60, disp.stop)
disp.start()
