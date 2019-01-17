import copy

from databroker.broker import Broker
from xpdan.pipelines.to_event_model import pipeline_order as tem_pipeline_order
from xpdan.pipelines.qoi import pipeline_order as qoi_pipeline_order
from xpdan.startup.analysis import (
    create_analysis_pipeline,
    pipeline_order,
    start_server,
)
from xpdtools.pipelines.extra import std_gen, z_score_gen
from xpdtools.pipelines.qoi import max_intensity_mean, max_gr_mean

db = Broker.named("live_demo_data")
db.prepare_hook = lambda x, y: copy.deepcopy(y)

order = (
    pipeline_order
    + [std_gen, z_score_gen, max_intensity_mean, max_gr_mean]
    + tem_pipeline_order
    + qoi_pipeline_order
)
namespace = create_analysis_pipeline(
    order, db=db, image_name="pe1_image", mask_setting={"setting": "first"}
)
start_server(namespace["raw_source"])
