from rapidz import Stream
from rapidz.link import link
from xpdtools.pipelines.raw_pipeline import *
from xpdan.pipelines.save import *


def pipeline_constructor(start_doc):
    """Constructs a pipeline from the start document

    Parameters
    ----------
    start_doc

    Returns
    -------

    """
    order = []
    namespace = {'raw_source': Stream(stream_name='raw source')}

    # if dark report nothing (empty pipeline for now)
    if 'is_dark' in start_doc:
        return

    # doc processing
    order.extend([doc_process, save_pipeline])
    # TODO: handle different detectors (or multiple at once!)
    # Setup standard image processing
    order.extend([em_image, image_process, save_tiff])

    # if calibration run add calibration pipeline
    # add carve out for when we don't want to run calibration,
    # maybe? or just let people filter it out?
    if 'detector_calibration_server_uid' in start_doc:
        order.extend([em_run_calibration,
                      run_calibration, save_geo])
    else:
        order.extend([em_calibration,
                      load_calibration, ])

    # add calibration handling
    order.append(geometry_handling)

    # add scattering corrections
    order.append(scattering_correction)

    # masking
    order.extend([gen_mask, save_mask])

    # integration
    order.extend([integration, save_iq])

    # if we have a composition we can run a PDF
    if 'composition_string' in start_doc:
        order.extend([pdf_gen, save_pdf])

    namespace.update(link(*order, **namespace))

    # return a callable that accepts (n, d) pairs
    return namespace['raw_source'].emit
