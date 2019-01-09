import copy

from databroker.broker import Broker
from xpdan.startup.analysis import start_analysis_server

db = Broker.named("live_demo_data")
db.prepare_hook = lambda x, y: copy.deepcopy(y)

start_analysis_server(db=db, image_name='pe1_image', mask_setting={'setting': 'first'})
