from bluesky.callbacks.zmq import RemoteDispatcher
from bluesky.utils import install_qt_kicker

from xpdconf.conf import glbl_dict
from xpdan.pipelines.main import raw_source
from xpdan.pipelines.main import (mask_kwargs as _mask_kwargs,
                                  pdf_kwargs as _pdf_kwargs,
                                  fq_kwargs as _fq_kwargs,
                                  mask_setting as _mask_setting)


def start_analysis(mask_kwargs=None,
                   pdf_kwargs=None, fq_kwargs=None, mask_setting=None):
    """Start analysis pipeline

    Parameters
    ----------
    mask_kwargs: dict
        The kwargs passed to the masking see xpdtools.tools.mask_img
    pdf_kwargs: dict
        The kwargs passed to the pdf generator, see xpdtools.tools.pdf_getter
    fq_kwargs: dict
        The kwargs passed to the fq generator, see xpdtools.tools.fq_getter
    mask_setting: dict
        The setting of the mask
    """
    d = RemoteDispatcher(glbl_dict['proxy_address'])
    install_qt_kicker(
        loop=d.loop)  # This may need to be d._loop depending on tag
    if mask_setting is None:
        mask_setting = {}
    if fq_kwargs is None:
        fq_kwargs = {}
    if pdf_kwargs is None:
        pdf_kwargs = {}
    if mask_kwargs is None:
        mask_kwargs = {}
    for a, b in zip([mask_kwargs, pdf_kwargs, fq_kwargs, mask_setting],
                    [_mask_kwargs, _pdf_kwargs, _fq_kwargs, _mask_setting]):
        if a:
            b.update(a)

    d.subscribe(raw_source.emit)

    d.start()
