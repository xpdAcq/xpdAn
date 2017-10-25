#!/usr/bin/env python
##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Timothy Liu, Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################

from xpdan.pipelines.main import conf_main_pipeline
from xpdan.pipelines.save_tiff import conf_save_tiff_pipeline
from xpdan.pipelines.callback import MainCallback
import matplotlib.pyplot as plt


def _prepare_header_list(headers):
    if not isinstance(headers, list):
        # still do it in two steps, easier to read
        header_list = list()
        header_list.append(headers)
    else:
        header_list = headers
    return header_list


def integrate_and_save(headers, *, db, save_dir, visualize=False,
                       polarization_factor=0.99, mask_setting='default',
                       mask_kwargs=None, image_data_key='pe1_image',
                       pdf_config=None):
    """Integrate and save dark subtracted images for given list of headers

    Parameters
    ----------
    headers : list
        a list of databroker.header objects
    db: databroker.broker.Broker instance
        The databroker holding the data, this must be specified as a `db=` in
        the function call (keyword only argument)
    save_dir: str
        The folder in which to save the data, this must be specified as a
        `save_dir=` in the function call (keyword only argument)
    visualize: bool, optional
        If True visualize the data. Defaults to False
    polarization_factor : float, optional
        polarization correction factor, ranged from -1(vertical) to +1
        (horizontal). default is 0.99. set to None for no
        correction.
    mask_setting : str optional
        If 'default' reuse mask created for first image, otherwise mask all
        images. Defaults to 'default'
    mask_kwargs : dict, optional
        dictionary stores options for automasking functionality.
        default is defined by an_glbl.auto_mask_dict.
        Please refer to documentation for more details
    image_data_key: str, optional
        The key for the image data, defaults to `pe1_image`
    pdf_config: dict, optional
        Configuration for making PDFs, see pdfgetx3 docs. Defaults to
        ``dict(dataformat='QA', qmaxinst=28, qmax=22)``

    Note
    ----
    complete docstring of masking functionality could be find in
    ``mask_img``

    See also
    --------
    xpdan.tools.mask_img
    """
    hdrs = _prepare_header_list(headers)
    source = MainCallback(db, save_dir, vis=visualize,
                          write_to_disk=True,
                          polarization_factor=polarization_factor,
                          image_data_key=image_data_key,
                          mask_setting=mask_setting,
                          mask_kwargs=mask_kwargs,
                          pdf_config=pdf_config)
    for hdr in hdrs:
        for nd in hdr.documents(fill=True):
            source(*nd)
    plt.close('all')


def integrate_and_save_last(**kwargs):
    """Integrate and save dark subtracted images for the latest header

    Parameters
    ----------
    headers : list
        a list of databroker.header objects
    db: databroker.broker.Broker instance
        The databroker holding the data, this must be specified as a `db=` in
        the function call (keyword only argument)
    save_dir: str
        The folder in which to save the data, this must be specified as a
        `save_dir=` in the function call (keyword only argument)
    visualize: bool, optional
        If True visualize the data. Defaults to False
    polarization_factor : float, optional
        polarization correction factor, ranged from -1(vertical) to +1
        (horizontal). default is 0.99. set to None for no
        correction.
    mask_setting : str optional
        If 'default' reuse mask created for first image, otherwise mask all
        images. Defaults to 'default'
    mask_kwargs : dict, optional
        dictionary stores options for automasking functionality.
        default is defined by an_glbl.auto_mask_dict.
        Please refer to documentation for more details
    image_data_key: str, optional
        The key for the image data, defaults to `pe1_image`
    pdf_config: dict, optional
        Configuration for making PDFs, see pdfgetx3 docs. Defaults to
        ``dict(dataformat='QA', qmaxinst=28, qmax=22)``

    Note
    ----
    complete docstring of masking functionality could be find in
    ``mask_img``

    See also
    --------
    xpdan.tools.mask_img
    """
    integrate_and_save(kwargs['db'][-1], **kwargs)
    plt.close('all')


def save_tiff(headers, *, db, save_dir,
              visualize=False,
              image_data_key='pe1_image'):
    """Save images obtained from dataBroker as tiff format files.

    Parameters
    ----------
    headers : list
        a list of databroker.header objects
    db: databroker.broker.Broker instance
        The databroker holding the data, this must be specified as a `db=` in
        the function call (keyword only argument)
    save_dir: str
        The folder in which to save the data, this must be specified as a
        `save_dir=` in the function call (keyword only argument)
    visualize: bool, optional
        If True visualize the data. Defaults to False
    image_data_key: str, optional
        The key for the image data, defaults to `pe1_image`
    """
    # normalize list
    hdrs = _prepare_header_list(headers)
    source = MainCallback(db=db, vis=visualize, save_dir=save_dir,
                          write_to_disk=True,
                          image_data_key=image_data_key,
                          analysis_setting='tiff only'
                          )
    for hdr in hdrs:
        for nd in hdr.documents(fill=True):
            source(*nd)
    plt.close('all')


def save_last_tiff(**kwargs):
    """Save images obtained from dataBroker as tiff format files.

    Parameters
    ----------
    headers : list
        a list of databroker.header objects
    db: databroker.broker.Broker instance
        The databroker holding the data, this must be specified as a `db=` in
        the function call (keyword only argument)
    save_dir: str
        The folder in which to save the data, this must be specified as a
        `save_dir=` in the function call (keyword only argument)
    visualize: bool, optional
        If True visualize the data. Defaults to False
    image_data_key: str, optional
        The key for the image data, defaults to `pe1_image`
    """

    save_tiff(kwargs['db'][-1], **kwargs)
    plt.close('all')
