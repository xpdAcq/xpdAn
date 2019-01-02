"""Example for XPD data"""
import os

import matplotlib.pyplot as plt
# pull from local data, not needed at beamline
from databroker.broker import Broker
from databroker._core import temp_config
import numpy as np
from tempfile import TemporaryDirectory
import copy
from pprint import pprint

db = Broker.named('live_demo_data')
db.prepare_hook = lambda x, y: copy.deepcopy(y)
td = TemporaryDirectory()

tmp = {'assets': {'config': {'dbpath': '/tmp/tmp5ucwapzn/assets.sqlite'}, 'class': 'Registry', 'module': 'databroker.assets.sqlite'}, 'description': 'temporary', 'metadatastore': {'config': {'directory': '/tmp/tmp5ucwapzn', 'timezone': 'US/Eastern'}, 'class': 'MDS', 'module': 'databroker.headersource.sqlite'}}

print(tmp)
db2 = Broker.from_config(tmp)

from xpdconf.conf import glbl_dict
glbl_dict.update(exp_db=db)

from rapidz import Stream
from rapidz.link import link

from shed.translation import ToEventStream, FromEventStream


def astype(x, ret_type='float32'):
    return x.astype(ret_type)


def pipeline(raw_source):
    b = (raw_source.map(astype).map(np.sum)
         # .sink(print)
         .ToEventStream(('sum', )).DBFriendly().starsink(db2.insert)
         )
    return locals()


namespace = link(pipeline,
                 raw_source=FromEventStream('event', ('data', 'pe1_image'),
                                            principle=True))

# vis = False
vis = True
# source.visualize(source_node=True)
# '''
for hdr in list((db[-1], )):
    for e in hdr.documents(fill=True):
        if e[0] == 'start':
            e[1].update(composition_string='EuTiO3')
        if e[0] == 'event' and vis:
            plt.pause(.1)
        if e[0] == 'event':
            if e[1]['seq_num'] > 3:
                continue
        namespace['raw_source'].update(e)
# print(db2[-1].start)
# print(db2[-1].stop)

# plt.show()
# plt.close("all")
# '''
# td.cleanup()


