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
from itertools import product
from pprint import pprint

import numpy as np
import pytest
import os

from xpdan.data_reduction import integrate_and_save, integrate_and_save_last, \
    save_tiff, save_last_tiff

integrate_params = [
    'polarization_factor',
    'mask_setting',
    'mask_kwargs',
]
good_kwargs = [
    (.99,),
    ('default', 'auto'),
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


@pytest.mark.parametrize(("kwargs", 'known_fail_bool'), integrate_kwargs)
def test_integrate_smoke(exp_db, fast_tmp_dir, disk_mask, kwargs,
                         known_fail_bool):
    old_files = os.listdir(fast_tmp_dir)
    old_times = [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
                 os.listdir(fast_tmp_dir)]
    if 'mask_setting' in kwargs.keys():
        if kwargs['mask_setting'] == 'use_saved_mask_msk':
            kwargs['mask_setting'] = disk_mask[0]
        elif kwargs['mask_setting'] == 'use_saved_mask':
            kwargs['mask_setting'] = disk_mask[1]
    elif 'mask_setting' in kwargs.keys() and kwargs['mask_setting'] == 'array':
        kwargs['mask_setting'] = np.random.random_integers(
            0, 1, disk_mask[-1].shape).astype(bool)
    kwargs['db'] = exp_db
    kwargs['save_dir'] = fast_tmp_dir
    pprint(kwargs)
    a = integrate_and_save(exp_db[-1], **kwargs)
    b = integrate_and_save_last(**kwargs)
    if known_fail_bool and not a and not b:
        pytest.xfail('Bad params')
    if kwargs.get('dryrun'):
        assert (
            set(old_files) != set(os.listdir(fast_tmp_dir)) and set(
                old_times) == set(
                [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
                 os.listdir(fast_tmp_dir)]))
    else:
        assert (
            set(old_files) != set(os.listdir(fast_tmp_dir)) or set(
                old_times) != set(
                [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
                 os.listdir(fast_tmp_dir)]))


@pytest.mark.parametrize(("kwargs", 'known_fail_bool'), save_tiff_kwargs)
def test_save_tiff_smoke(exp_db, fast_tmp_dir, kwargs, known_fail_bool):
    old_files = os.listdir(fast_tmp_dir)
    old_times = [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
                 os.listdir(fast_tmp_dir)]
    kwargs['db'] = exp_db
    kwargs['save_dir'] = fast_tmp_dir
    pprint(kwargs)
    a = save_tiff(exp_db[-1], **kwargs)
    b = save_last_tiff(**kwargs)
    if known_fail_bool and not a and not b:
        pytest.xfail('Bad params')
    if kwargs.get('dryrun'):
        assert (
            set(old_files) != set(os.listdir(fast_tmp_dir)) and
            set(old_times) == set(
                [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
                 os.listdir(fast_tmp_dir)]))
    else:
        assert (
            set(old_files) != set(os.listdir(fast_tmp_dir)) or set(
                old_times) != set(
                [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
                 os.listdir(fast_tmp_dir)]))
