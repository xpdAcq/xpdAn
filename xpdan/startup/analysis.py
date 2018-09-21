"""Tools for starting and managing the analysis server"""
from bluesky.callbacks.zmq import RemoteDispatcher
from bluesky.utils import install_qt_kicker
from xpdconf.conf import glbl_dict

from xpdan.pipelines.main import pipeline_order
from xpdan.pipelines.save import pipeline_order as save_pipeline_order
from xpdan.pipelines.vis import vis_pipeline
from streamz_ext.link import link
from streamz_ext import Stream


def start_analysis(**kwargs):
    """Start analysis pipeline

    Parameters
    ----------
    mask_kwargs : dict
        The kwargs passed to the masking see xpdtools.tools.mask_img
    pdf_kwargs : dict
        The kwargs passed to the pdf generator, see xpdtools.tools.pdf_getter
    fq_kwargs : dict
        The kwargs passed to the fq generator, see xpdtools.tools.fq_getter
    mask_setting : dict
        The setting of the mask
    save_template : str
        The template string for file saving
    base_folder : str
        The base folder for saving files
    """
    # TODO: also start up grave vis
    d = RemoteDispatcher(glbl_dict["proxy_address"])
    install_qt_kicker(
        loop=d.loop
    )  # This may need to be d._loop depending on tag
    namespace = link(
        *(pipeline_order + save_pipeline_order + [vis_pipeline]),
        raw_source=Stream(stream_name="raw source"),
        **kwargs
    )
    raw_source = namespace["raw_source"]
    d.subscribe(lambda *x: raw_source.emit(x))
    print("Starting Analysis Server")
    d.start()
