"""Example for XPD data"""
import os

import matplotlib.pyplot as plt
import tzlocal

from databroker.assets.handlers import AreaDetectorTiffHandler
from databroker.broker import Broker
# pull from local data, not needed at beamline
from databroker.assets.sqlite import RegistryRO
from databroker.headersource.sqlite import MDSRO
from xpdan.pipelines.master import conf_master_pipeline

# from xpdan.tools import better_mask_img

d = {'directory': '/home/christopher/live_demo_data',
     'timezone': tzlocal.get_localzone().zone,
     'dbpath': os.path.join('/home/christopher/live_demo_data', 'filestore')}
mds = MDSRO(d)
fs = RegistryRO(d)
fs.register_handler('AD_TIFF', AreaDetectorTiffHandler)
db = Broker(mds=mds, reg=fs)
source = conf_master_pipeline(db)
# source.visualize()
seen = False
for e in db[-1].stream(fill=True):
    if e[0] == 'event':
        plt.pause(.5)
        if not seen:
            seen = True
            source.emit(e)
    else:
        source.emit(e)

print('start second run ----------------------------------------------------')

seen = False
for e in db[-1].stream(fill=True):
    if e[0] == 'event':
        plt.pause(.5)
        if not seen:
            seen = True
            source.emit(e)
    else:
        source.emit(e)

plt.show()
plt.close("all")
