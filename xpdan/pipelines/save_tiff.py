from xpdtools.pipelines.raw_pipeline import image_process

from xpdan.pipelines.main import start_gen
from xpdan.pipelines.save import save_pipeline, save_tiff

pipeline_order = [start_gen, image_process, save_pipeline, save_tiff]
