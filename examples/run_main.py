"""Example for XPD data"""
import os

import matplotlib.pyplot as plt
# pull from local data, not needed at beamline
from databroker.broker import Broker
from tempfile import TemporaryDirectory
import copy
from pprint import pprint

db = Broker.named('live_demo_data')
db.prepare_hook = lambda x, y: copy.deepcopy(y)
td = TemporaryDirectory()

from xpdconf.conf import glbl_dict
glbl_dict.update(exp_db=db)

from xpdan.pipelines.main import *
from xpdan.pipelines.qoi import *
from xpdan.pipelines.vis import *

# vis = False
vis = True
# source.visualize(source_node=True)
# source.visualize(source_node=False)
# '''
for hdr in list((db[-1], )):
    for e in hdr.documents(fill=True):
        if e[0] == 'start':
            e[1].update(composition_string='EuTiO3')
        if e[0] == 'event' and vis:
            plt.pause(.1)
        if e[0] == 'event':
            if e[1]['seq_num'] > 3:
                # AAA
                pass
        raw_source.emit(e)


plt.show()
plt.close("all")
# '''
td.cleanup()
