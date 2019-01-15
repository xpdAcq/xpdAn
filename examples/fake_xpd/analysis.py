import copy

from databroker.broker import Broker
from xpdan.startup.analysis import start_analysis_server

from bluesky.callbacks.zmq import RemoteDispatcher
from bluesky.utils import install_qt_kicker
from xpdan.pipelines.to_em import to_em, to_event_stream_with_ind
from xpdan.vend.callbacks.zmq import Publisher
from xpdconf.conf import glbl_dict

from xpdan.pipelines.main import pipeline_order
from xpdtools.pipelines.extra import std_gen, z_score_gen
from xpdtools.pipelines.qoi import max_intensity_mean, max_gr_mean
from xpdan.pipelines.save import pipeline_order as save_pipeline_order
from xpdan.pipelines.vis import vis_pipeline
from rapidz.link import link
from rapidz import Stream
from xpdan.startup.analysis import *

db = Broker.named("live_demo_data")
db.prepare_hook = lambda x, y: copy.deepcopy(y)

order = (
        pipeline_order
        + [std_gen, z_score_gen, max_intensity_mean, max_gr_mean]
        + [to_em]
    )
namespace = create_analysis_pipeline(order, 
                                     db=db, 
                                     image_name='pe1_image', 
                                     mask_setting={'setting': 'first'})
start_analysis_server(namespace['raw_source'])

