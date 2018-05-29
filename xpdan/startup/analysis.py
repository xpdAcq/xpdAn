"""Tools for starting and managing the analysis server"""
from bluesky.callbacks.zmq import RemoteDispatcher
from bluesky.utils import install_qt_kicker
from xpdconf.conf import glbl_dict

from xpdan.pipelines.main import *  # noqa: F403, F401
from xpdan.pipelines.save import *  # noqa: F403, F401
from xpdan.pipelines.vis import *  # noqa: F403, F401
# from xpdan.pipelines.qoi import *  # noqa: F403, F401
from xpdan.pipelines.main import (mask_kwargs as _mask_kwargs,
                                  pdf_kwargs as _pdf_kwargs,
                                  fq_kwargs as _fq_kwargs,
                                  mask_setting as _mask_setting)
from xpdan.pipelines.save import save_kwargs as _save_kwargs
# from xpdan.pipelines.qoi import (
#     pdf_argrelmax_kwargs as _pdf_argrelmax_kwargs,
#     mean_argrelmax_kwargs as _mean_argrelmax_kwargs)


def start_analysis(mask_kwargs=None,
                   pdf_kwargs=None, fq_kwargs=None, mask_setting=None,
                   save_kwargs=None,
                   # pdf_argrelmax_kwargs=None,
                   # mean_argrelmax_kwargs=None
                   ):
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
    save_kwargs : dict
        The kwargs passed to the main formatting node (mostly the filename
        template)
    """
    # if pdf_argrelmax_kwargs is None:
    #     pdf_argrelmax_kwargs = {}
    # if mean_argrelmax_kwargs is None:
    #     mean_argrelmax_kwargs = {}
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
    if save_kwargs is None:
        save_kwargs = {}
    for a, b in zip([mask_kwargs, pdf_kwargs, fq_kwargs, mask_setting,
                     save_kwargs,
                     # pdf_argrelmax_kwargs,
                     # mean_argrelmax_kwargs
                     ],
                    [_mask_kwargs, _pdf_kwargs, _fq_kwargs, _mask_setting,
                     _save_kwargs,
                     # _pdf_argrelmax_kwargs,
                     # _mean_argrelmax_kwargs
                     ]):
        if a:
            b.update(a)

    d.subscribe(lambda *x: raw_source.emit(x))
    print('Starting Analysis Server')
    d.start()
