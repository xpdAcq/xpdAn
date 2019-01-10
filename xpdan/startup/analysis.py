"""Tools for starting and managing the analysis server"""
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


def start_analysis(save=True, vis=True, **kwargs):
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
    # TODO: also start up grave vis, maybe?
    d = RemoteDispatcher(glbl_dict["outbound_proxy_address"])
    install_qt_kicker(
        loop=d.loop
    )  # This may need to be d._loop depending on tag
    order = pipeline_order
    if save:
        order += save_pipeline_order
    if vis:
        order += [vis_pipeline]
    namespace = link(
        *order, raw_source=Stream(stream_name="raw source"), **kwargs
    )
    raw_source = namespace["raw_source"]
    d.subscribe(lambda *x: raw_source.emit(x))
    print("Starting Analysis Server")
    d.start()


def create_analysis_pipeline(order, **kwargs):
    namespace = link(
        *order, raw_source=Stream(stream_name="raw source"), **kwargs
    )
    raw_source = namespace["raw_source"]

    # do inspection of pipeline for ToEventModel nodes, maybe?
    # for analyzed data with independent data (vis and save)
    an_with_ind_pub = Publisher(
        glbl_dict["inbound_proxy_address"], prefix=b"vis"
    )
    namespace.update(
        to_event_stream_with_ind(
            raw_source,
            *[
                namespace[k]
                for k in [
                    "dark_corrected_tes",
                    # "geometry_tes",
                    # "mask_tes",
                    "integration_tes",
                    "pdf_tes",
                    "max_tes",
                    "max_pdf_tes",
                ]
            ],
            publisher=an_with_ind_pub
        )
    )

    # for analyzed data without independent data (analysis db)
    an_no_ind_pub = Publisher(
        glbl_dict["inbound_proxy_address"], prefix=b"an_no_ind"
    )

    namespace.update(
        to_event_stream_with_ind(
            raw_source,
            *[
                namespace[k]
                for k in [
                    # "dark_corrected_tes",
                    # "geometry_tes",
                    # "mask_tes",
                    "integration_tes",
                    "pdf_tes",
                ]
            ],
            publisher=an_no_ind_pub
        )
    )
    return namespace


def start_analysis_server(raw_source):
    d = RemoteDispatcher(
        glbl_dict["outbound_proxy_address"],
        # accept the raw data
        prefix=b"raw",
    )
    install_qt_kicker(
        loop=d.loop
    )

    d.subscribe(lambda *x: raw_source.emit(x))
    print("Starting Analysis Server")
    d.start()


if __name__ == '__main__':  # pragma: no cover
    order = (
        pipeline_order
        + [std_gen, z_score_gen, max_intensity_mean, max_gr_mean]
        + [to_em]
    )
    namespace = create_analysis_pipeline(order, **kwargs)
    start_analysis_server(namespace['raw_source'])
