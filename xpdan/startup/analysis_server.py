"""Server for analyzing data at XPD

Notes
-----
This module can be called via a ``fire`` cli or used interactively.
"""
import copy
from warnings import warn

import fire

from bluesky.utils import install_qt_kicker
from rapidz import Stream
from rapidz.link import link
from xpdan.pipelines.extra import z_score_tem
from xpdan.pipelines.main import pipeline_order
from xpdan.pipelines.qoi import pipeline_order as qoi_pipeline_order
from xpdan.pipelines.save import pipeline_order as save_pipeline_order
from xpdan.pipelines.to_event_model import (
    pipeline_order as tem_pipeline_order,
    to_event_stream_no_ind,
)
from xpdan.pipelines.to_event_model import to_event_stream_with_ind
from xpdan.pipelines.vis import vis_pipeline
from xpdan.vend.callbacks.core import StripDepVar
from xpdan.vend.callbacks.zmq import Publisher, RemoteDispatcher
from xpdconf.conf import glbl_dict
from xpdtools.pipelines.extra import std_gen, median_gen, z_score_gen
from xpdtools.pipelines.qoi import max_intensity_mean, max_gr_mean

order = (
    pipeline_order
    + [
        # std_gen,
        # median_gen,
        # z_score_gen,
        # z_score_tem,
        max_intensity_mean,
        max_gr_mean,
    ]
    + tem_pipeline_order
    + qoi_pipeline_order
)


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
    warn(DeprecationWarning("Use the server instead"))
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
        glbl_dict["inbound_proxy_address"], prefix=b"an")
    # strip the dependant vars form the raw data
    raw_stripped = raw_source.starmap(StripDepVar())
    # TODO: inspect this from the namespace
    #  look for SimpleToEventStream nodes
    namespace.update(
        to_event_stream_with_ind(
            raw_stripped,
            *[
                namespace[k]
                for k in [
                    "dark_corrected_tes",
                    "bg_corrected_tes",
                    "geometry_tes",
                    "mask_tes",
                    "integration_tes",
                    "fq_tes",
                    "sq_tes",
                    "mask_overlay_tes",
                    "pdf_tes",
                    "max_tes",
                    "max_pdf_tes",
                    "z_score_tes",
                ]
                if k in namespace
            ],
            publisher=an_with_ind_pub
        )
    )
    # '''

    """
    an_with_no_ind_pub = Publisher(
        glbl_dict["inbound_proxy_address"], prefix=b"clean_an",
        serializer=serializer
    )
    namespace.update(
        to_event_stream_no_ind(
            *[
                namespace[k]
                for k in [
                    # "dark_corrected_tes",
                    # "bg_corrected_tes",
                    # XXX: reinstate this when pyfai has a saver
                    # "geometry_tes",
                    # "mask_tes",
                    "integration_tes",
                    "fq_tes",
                    "sq_test",
                    # "mask_overlay_tes",
                    "pdf_tes",
                    "max_tes",
                    "max_pdf_tes",
                ]
                if k in namespace
            ],
            publisher=an_with_no_ind_pub
        )
    )
    # """
    return namespace


def run_server(
    order=order,
    db=glbl_dict["exp_db"],
    outbound_proxy_address=glbl_dict["outbound_proxy_address"],
    prefix=b"raw",
    **kwargs
):
    db.prepare_hook = lambda x, y: copy.deepcopy(y)

    if "db" not in kwargs:
        kwargs.update(db=db)

    namespace = create_analysis_pipeline(order, **kwargs)

    d = RemoteDispatcher(
        outbound_proxy_address,
        # accept the raw data
        prefix=prefix,
    )
    install_qt_kicker(loop=d.loop)

    d.subscribe(lambda *x: namespace["raw_source"].emit(x))
    print("Starting Analysis Server")
    d.start()


def run_main():
    fire.Fire(run_server)


if __name__ == "__main__":
    run_main()
