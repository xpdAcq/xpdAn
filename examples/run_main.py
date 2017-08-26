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
                            vis=False,
                            write_to_disk=False
                            )
source.visualize()
for hdr in list((db[-1], )):
    for e in hdr.documents():
        if e[0] == 'event':
            # plt.pause(.1)
            pass
        source.emit(e)

plt.show()
plt.close("all")
td.cleanup()
