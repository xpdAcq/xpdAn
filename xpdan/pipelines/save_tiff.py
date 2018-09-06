from xpdan.pipelines.main import start_gen
from xpdan.pipelines.save import save_pipeline, save_tiff
from xpdtools.pipelines.raw_pipeline import image_process

pipeline_order = [start_gen, image_process, save_pipeline, save_tiff]
