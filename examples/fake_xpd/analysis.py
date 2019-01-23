import copy

from databroker.broker import Broker
from xpdan.startup.analysis_server import order, run_server

db = Broker.named("live_demo_data")
db.prepare_hook = lambda x, y: copy.deepcopy(y)

run_server(order=order, db=db, mask_setting={"setting": "first"})
