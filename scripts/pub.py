"""Example for XPD data"""
import os

import matplotlib.pyplot as plt
import tzlocal

from databroker.assets.handlers import AreaDetectorTiffHandler
from databroker.broker import Broker
# pull from local data, not needed at beamline
from databroker.assets.sqlite import RegistryRO
from databroker.headersource.sqlite import MDSRO
from xpdan.pipelines.main import conf_main_pipeline
from tempfile import TemporaryDirectory
from bluesky.callbacks.zmq import Proxy, Publisher, RemoteDispatcher
import time
import multiprocessing
from shed.event_streams import istar
from shed.utils import to_event_model
import copy
import numpy as np

# from xpdan.tools import better_mask_img

d = {'directory': '/home/christopher/live_demo_data',
     'timezone': tzlocal.get_localzone().zone,
     'dbpath': os.path.join('/home/christopher/live_demo_data', 'filestore')}
mds = MDSRO(d)
fs = RegistryRO(d)
fs.register_handler('AD_TIFF', AreaDetectorTiffHandler)
db = Broker(mds=mds, reg=fs)

p = Publisher('127.0.0.1:5567')  # noqa
db.prepare_hook = lambda a, x: copy.deepcopy(x)
# '''
for name, doc in db[-1].documents():
    p(name, doc)
    time.sleep(10)
#    input()
'''
g = to_event_model([np.random.random((10, 10)) for _ in range(10)],
                   output_info=[('pe1_image', {'dtype': 'array',
                                               'shape': (10, 10)})])
for name, doc in g:
    print(doc)
    p(name, doc)
# '''
