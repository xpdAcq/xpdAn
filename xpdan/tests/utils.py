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
import os
import tempfile
import time
from uuid import uuid4

import numpy as np

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


def insert_imgs(mds, fs, n, shape, save_dir=tempfile.mkdtemp(),
                **kwargs):
    """
    Insert images into mds and fs for testing

    Parameters
    ----------
    mds
    fs
    n
    shape
    save_dir

    Returns
    -------

    """
    beamtime_uid = str(uuid4())
    # Insert the dark images
    dark_img = np.ones(shape)
    dark_uid = str(uuid4())
    run_start = mds.insert_run_start(uid=dark_uid, time=time.time(),
                                     name='test-dark',
                                     beamtime_uid=beamtime_uid,
                                     sample_name='hi',
                                     calibration_md=pyFAI_calib,
                                     **kwargs)
    data_keys = {
        'pe1_image': dict(source='testing', external='FILESTORE:',
                          dtype='array')}
    data_hdr = dict(run_start=run_start,
                    data_keys=data_keys,
                    time=time.time(), uid=str(uuid4()))
    descriptor = mds.insert_descriptor(**data_hdr)
    for i, img in enumerate([dark_img]):
        fs_uid = str(uuid4())
        fn = os.path.join(save_dir, fs_uid + '.npy')
        np.save(fn, img)
        # insert into FS
        fs_res = fs.insert_resource('npy', fn, resource_kwargs={})
        fs.insert_datum(fs_res, fs_uid, datum_kwargs={})
        mds.insert_event(
            descriptor=descriptor,
            uid=str(uuid4()),
            time=time.time(),
            data={'pe1_image': fs_uid},
            timestamps={},
            seq_num=i)
    mds.insert_run_stop(run_start=run_start,
                        uid=str(uuid4()),
                        time=time.time())

    imgs = [np.ones(shape)] * n
    run_start = mds.insert_run_start(uid=str(uuid4()), time=time.time(),
                                     name='test', sc_dk_field_uid=dark_uid,
                                     beamtime_uid=beamtime_uid,
                                     sample_name='hi',
                                     calibration_md=pyFAI_calib,
                                     **kwargs)
    data_keys = {
        'pe1_image': dict(source='testing', external='FILESTORE:',
                          dtype='array')}
    data_hdr = dict(run_start=run_start,
                    data_keys=data_keys,
                    time=time.time(), uid=str(uuid4()))
    descriptor = mds.insert_descriptor(**data_hdr)
    for i, img in enumerate(imgs):
        fs_uid = str(uuid4())
        fn = os.path.join(save_dir, fs_uid + '.npy')
        np.save(fn, img)
        # insert into FS
        fs_res = fs.insert_resource('npy', fn, resource_kwargs={})
        fs.insert_datum(fs_res, fs_uid, datum_kwargs={})
        mds.insert_event(
            descriptor=descriptor,
            uid=str(uuid4()),
            time=time.time(),
            data={'pe1_image': fs_uid},
            timestamps={'pe1_image': time.time()},
            seq_num=i)
    mds.insert_run_stop(run_start=run_start,
                        uid=str(uuid4()),
                        time=time.time())
    return save_dir
