import copy

from databroker.broker import Broker
from xpdan.startup.analysis_server import ORDER, analysis_server

db = Broker.named("live_demo_data")
db.prepare_hook = lambda x, y: copy.deepcopy(y)

analysis_server(order=ORDER, db=db, mask_setting={"setting": "first"})
