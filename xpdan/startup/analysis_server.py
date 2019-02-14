"""Server for analyzing data at XPD

Notes
-----
This module can be called via a ``fire`` cli or used interactively.
"""
import copy
from warnings import warn

import fire

from bluesky.utils import install_qt_kicker
from rapidz import Stream, move_to_first
from rapidz.link import link
from shed import SimpleToEventStream
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
    """Start analysis pipeline [Depreciated]

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


def create_analysis_pipeline(
        order,
        inbound_proxy_address=glbl_dict["inbound_proxy_address"],
        **kwargs):
    """Create the analysis pipeline from an list of chunks and pipeline kwargs

    Parameters
    ----------
    order : list of functions
        The list of pipeline chunk functions
    kwargs : Any
        The kwargs to pass to the pipeline creation

    Returns
    -------
    namespace : dict
        The namespace of the pipeline

    """
    namespace = link(
        *order, raw_source=Stream(stream_name="raw source"), **kwargs
    )
    source = namespace["source"]

    # do inspection of pipeline for ToEventModel nodes, maybe?
    # for analyzed data with independent data (vis and save)
    an_with_ind_pub = Publisher(
        inbound_proxy_address, prefix=b"an"
    )
    # strip the dependant vars form the raw data
    raw_stripped = move_to_first(source.starmap(StripDepVar()))
    namespace.update(
        to_event_stream_with_ind(
            raw_stripped,
            *[
                node
                for node in namespace.values()
                if isinstance(node, SimpleToEventStream)
            ],
            publisher=an_with_ind_pub
        )
    )

    return namespace


def run_server(
    order=order,
    db=glbl_dict["exp_db"],
    outbound_proxy_address=glbl_dict["outbound_proxy_address"],
    inbound_proxy_address=glbl_dict["inbound_proxy_address"],
    prefix=b"raw",
    plot_graph=False,
    **kwargs
):
    """Function to run the analysis server.

    Parameters
    ----------
    order : list, optional
        The order of pipeline chunk functions to be called. Defaults to the
        standard order, ``xpdan.startup.analysis_server.order``
    db : databroker.Broker instance, optional
        The databroker to pull data from. This is used for accessing dark and
        background data. Defaults to the location listed in the
        ``xpdconf.conf.glbl_dict``.
    outbound_proxy_address : str, optional
        The location of the ZMQ proxy sending data to this server. Defaults
        to the location listed in the ``xpdconf.conf.glbl_dict``.
    prefix : bytes or list of bytes, optional
        Which publisher(s) to listen to for data. Defaults to ``b"raw"``
    kwargs : Any
        Keyword arguments passed into the pipeline creation. These are used
        to modify the data processing.

    If using the default pipeline these include:

      - ``bg_scale=1`` The background scale factor. Defaults to 1
      - ``calib_setting`` : The calibration setting, if set to
        ``{"setting": False}`` the user will not be prompted to perform
        calibration on calibration samples.
        This is useful for not performing calibration when re analyzing an
        entire experiment.
      - ``polarization_factor`` The polarization factor used to correct
        the image. Defaults to .99
      - ``mask_setting`` The setting for the frequency of the mask. If set
        to ``{'setting': 'auto'}`` each image gets a mask generated for it,
        if set to ``{'setting': 'first'}`` only the first image in the
        series has a mask generated for it and all subsequent images in the
        series use that mask, if set to ``{'setting': 'none'}`` then no
        image is masked. Defaults to ``{'setting': 'auto'}``.
      - ``mask_kwargs`` The keyword arguments passed to
        ``xpdtools.tools.mask_img``. Defaults to ``dict(edge=30,
        lower_thresh=0.0, upper_thresh=None, alpha=3, auto_type="median",
        tmsk=None,)``
      - kwargs passed to PDFgetx3. Please see the PDFgetx3 documentation:
        https://www.diffpy.org/doc/pdfgetx/2.0.0/options.html#pdf-parameters
    """
    print(kwargs)
    db.prepare_hook = lambda x, y: copy.deepcopy(y)

    if "db" not in kwargs:
        kwargs.update(db=db)

    namespace = create_analysis_pipeline(
        order,
        inbound_proxy_address=inbound_proxy_address,
        **kwargs)

    d = RemoteDispatcher(
        outbound_proxy_address,
        # accept the raw data
        prefix=prefix,
    )
    install_qt_kicker(loop=d.loop)

    d.subscribe(lambda *x: namespace["raw_source"].emit(x))
    if plot_graph:
        namespace['raw_source'].visualize(source_node=True)
    print("Starting Analysis Server")
    d.start()


def run_main():
    fire.Fire(run_server)


if __name__ == "__main__":
    run_main()
