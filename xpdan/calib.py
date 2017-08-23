#!/usr/bin/env python
##############################################################################
#
# xpdacq            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Timothy Liu
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
import os
import uuid
import time
import yaml
import datetime
import numpy as np

from xpdan.tools import _timestampstr

from pyFAI.gui.utils import update_fig
from pyFAI.calibration import Calibration, PeakPicker


def _configure_calib_instance(calibrant, detector, wavelength):
    """function to configure calibration instance"""
    c = Calibration(calibrant=calibrant, detector=detector,
                    wavelength=wavelength * 10 ** (-10))

    return c, c.calibrant.dSpacing


def _save_calib_param(calib_c, timestr, calib_yml_fp):
    """save calibration parameters to designated location

    Parameters
    ----------
    calib_c : pyFAI.calibration.Calibration instance
        pyFAI Calibration instance with parameters after calibration
    time_str : str
        human readable time string
    calib_yml_fp : str
        filepath to the yml file which stores calibration param
    """
    # save glbl attribute for xpdAcq
    calibrant_name = calib_c.calibrant.__repr__().split(' ')[0]
    calib_config_dict = {}
    calib_config_dict = calib_c.geoRef.getPyFAI()
    calib_config_dict.update(calib_c.geoRef.getFit2D())
    calib_config_dict.update({'poni_file_name':
                              calib_c.basename+'.poni'})
    calib_config_dict.update({'time':timestr})
    calib_config_dict.update({'dSpacing':
                              calib_c.calibrant.dSpacing})
    calib_config_dict.update({'calibrant_name':
                              calibrant_name})

    # save yaml dict used for xpdAcq
    with open(calib_yml_fp, 'w') as f:
        yaml.dump(calib_config_dict, f)
    stem, fn = os.path.split(calib_yml_fp)
    print("INFO: End of calibration process. Your parameter set will be "
          "saved inside {}. this set of parameters will be injected "
          "as metadata to subsequent scans until you perform this "
          "process again\n".format(fn))
    print("INFO: you can also use:\n>>> show_calib()\ncommand to check"
          " current calibration parameters")
    #print("INFO: To save your calibration image as a tiff file run\n"
    #      "save_last_tiff()\nnow.")
    return calib_config_dict


def _calibration(img, calibration, save_dir, **kwargs):
    """engine for performing calibration on a image with geometry
    correction software. current backend is ``pyFAI``.

    Parameters
    ----------
    img : ndarray
        image to perfrom calibration process.
    calibration : pyFAI.calibration.Calibration instance
        pyFAI Calibration instance with wavelength, calibrant and
        detector configured.
    save_dir : str
        directory where the poni file will be saved.
    kwargs:
        additional keyword argument for calibration. please refer to
        pyFAI documentation for all options.
    """
    print('{:=^20}'.format("INFO: you are able to perform calibration, "
                           "please refer to pictorial guide here:\n"))
    print('{:^20}'
          .format("http://xpdacq.github.io/usb_Running.html#calib-manual\n"))
    # default params
    interactive = True
    dist = 0.1
    # calibration
    c = calibration  # shorthand notation
    timestr = _timestampstr(time.time())
    f_name = '_'.join([timestr, 'pyFAI_calib',
                       c.calibrant.__repr__().split(' ')[0]])
    w_name = os.path.join(save_dir, f_name)  # poni name
    poni_name = w_name + ".npt"
    c.gui = interactive
    c.basename = w_name
    c.pointfile = poni_name
    c.peakPicker = PeakPicker(img, reconst=True,
                              pointfile=poni_name,
                              calibrant=c.calibrant,
                              wavelength=c.wavelength,
                              **kwargs)
    c.peakPicker.gui(log=True, maximize=True, pick=True)
    update_fig(c.peakPicker.fig)
    c.gui_peakPicker()

    return c, timestr

#NOTE: following function is not finished yet.
def img_calibration(img, wavelength, calibrant=None,
                    detector=None, **kwargs):
    """function to calibrate experimental geometry wrt an image

    Parameters
    ----------
    img : ndarray
        2D powder diffraction image from calibrant
    wavelength : float
        x-ray wavelength in angstrom.
    calibrant : str, optional
        calibrant being used, default is 'Ni'.
        input could be ``full file path'' to customized d-spacing file with
        ".D" extension or one of pre-defined calibrant names.
        List of pre-defined calibrant names is:
        ['NaCl', 'AgBh', 'quartz', 'Si_SRM640', 'Ni', 'Si_SRM640d',
         'Si_SRM640a', 'alpha_Al2O3', 'LaB6_SRM660b', 'TiO2', 'CrOx',
         'LaB6_SRM660c', 'CeO2', 'Si_SRM640c', 'CuO', 'Si_SRM640e',
         'PBBA', 'ZnO', 'Si', 'C14H30O', 'cristobaltite', 'LaB6_SRM660a',
         'Au', 'Cr2O3', 'Si_SRM640b', 'LaB6', 'Al', 'mock']
    detector : str or pyFAI.detector.Detector instance, optional.
        detector used to collect data. default value is 'perkin-elmer'.
        other allowed values are in pyFAI documentation.
    kwargs:
        Additional keyword argument for calibration. please refer to
        pyFAI documentation for all options.

    Returns
    -------
    ai : pyFAI.AzimuthalIntegrator
        instance of AzimuthalIntegrator. can be used to integrate 2D
        images directly.
    Examples
    --------
    # calib Ni image with pyFAI default ``Ni.D`` d-spacing
    # with wavlength 0.1823 angstrom
    >>> import tifffile as tif
    >>> ni_img = tif.imread(<path_to_img_file>)
    >>> ai = img_calibration(ni_img, 0.1823)

    # calib Ni image with pyFAI customized ``myNi.D`` d-spacing
    # with wavlength 0.1823 angstrom
    >>> import tifffile as tif
    >>> ni_img = tif.imread(<path_to_img_file>)
    >>> ai = img_calibration(ni_img, 0.1823, 'path/to/myNi.D')

    # integrate image right after calibration
    >>> import matplotlib.pyplot as plt
    >>> npt = 1482 # just a number for demonstration
    >>> q, Iq = ai.integrate1d(ni_img, npt, unit="q_nm^-1")
    >>> plt.plot(q, Iq)

    Reference
    ---------
    pyFAI documentation:
    http://pyfai.readthedocs.io/en/latest/
    """
    # configure calibration instance
    c = _configure_calib_instance(calibrant, detector, wavelength)
    # pyFAI calibration
    calib_c, timestr = _calibration(img, c, **kwargs)

    return calib_c.ai
