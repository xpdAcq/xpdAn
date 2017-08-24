##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
import tempfile
from itertools import product
from uuid import uuid4

import numpy as np

from bluesky.examples import ReaderWithRegistry
from bluesky.plans import count

pyFAI_calib = {'calibration_collection_uid': 'uuid1234',
               'centerX': 1019.8886820814655,
               'centerY': 1026.5636273165978,
               'detector': 'Perkin detector',
               'directDist': 208.36071181911709,
               'dist': 0.208359323484,
               'file_name': 'pyFAI_calib_Ni_20160813-1659.poni',
               'pixel1': 0.0002,
               'pixel2': 0.0002,
               'pixelX': 200.0,
               'pixelY': 200.0,
               'poni1': 0.204863292224,
               'poni2': 0.203364094157,
               'rot1': -0.00294510691846,
               'rot2': 0.00215699775598,
               'rot3': -8.04331174483e-08,
               'splineFile': None,
               'tilt': 0.20915926627927123,
               'tiltPlanRotation': 36.219147551081498,
               'time': '20160813-1815',
               'wavelength': 1.8333e-11}


def insert_imgs(RE, reg, n, shape, save_dir=tempfile.mkdtemp(), **kwargs):
    """
    Insert images into mds and fs for testing

    Parameters
    ----------
    RE: bluesky.run_engine.RunEngine instance
    reg: Registry instance
    n: int
        Number of images to take
    shape: tuple of ints
        The shape of the resulting images
    save_dir

    Returns
    -------

    """
    # Create detectors
    dark_det = ReaderWithRegistry('pe1_image',
                                  {'pe1_image': lambda: np.ones(shape)},
                                  reg=reg, save_path=save_dir)
    light_det = ReaderWithRegistry('pe1_image',
                                   {'pe1_image': lambda: np.ones(shape)},
                                   reg=reg, save_path=save_dir)
    beamtime_uid = str(uuid4())
    base_md = dict(beamtime_uid=beamtime_uid,
                   calibration_md=pyFAI_calib,
                   bt_wavelength=0.1847,
                   **kwargs)

    # Insert the dark images
    dark_md = base_md.copy()
    dark_md.update(name='test-dark', is_dark=True)

    dark_uid = RE(count([dark_det], num=1), **dark_md)

    # Insert the light images
    light_md = base_md.copy()
    light_md.update(name='test', sc_dk_field_uid=dark_uid)
    uid = RE(count([light_det], num=n), **light_md)
    print(dark_uid, uid)

    return uid


class PDFGetterShim:
    def __init__(self):
        self.config = {'qmax': 'testing'}
        self.fq = np.ones(10), np.ones(10)

    def __call__(self, *args, **kwargs):
        print("This is a testing shim for PDFgetx if you see this message then"
              "you don't have PDFgetx3 installed. "
              "The data that comes from this is for testing purposes only"
              "and has no bearing on reality")
        return np.ones(10), np.ones(10)


integrate_params = [
    'polarization_factor',
    'mask_setting',
    'mask_kwargs',
]
good_kwargs = [
    (.99,),
    (
        # 'default',
        # 'auto',
        None,
    ),
    [None, {'alpha': 3}],
]

bad_integrate_params = ['polarization_factor',
                        'mask_setting',
                        'mask_kwargs']

bad_kwargs = [['str'] for i in range(len(bad_integrate_params))]

integrate_kwarg_values = product(*good_kwargs)
integrate_kwargs = []
for vs in integrate_kwarg_values:
    d = {k: v for (k, v) in zip(integrate_params, vs)}
    integrate_kwargs.append((d, False))

for vs in bad_kwargs:
    d = {k: v for (k, v) in zip(bad_integrate_params, vs)}
    integrate_kwargs.append((d, True))

save_tiff_kwargs = []
for d in [save_tiff_kwargs, integrate_kwargs]:
    for d2 in d:
        d2[0]['image_data_key'] = 'pe1_image'
