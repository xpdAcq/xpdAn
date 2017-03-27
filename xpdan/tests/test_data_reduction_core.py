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
from itertools import tee, product
from pprint import pprint

import numpy as np
import pytest
import os

from xpdan.data_reduction_core import integrate_and_save, sum_images, \
    integrate_and_save_last, save_tiff, save_last_tiff

sum_idx_values = (
    None, 'all', [1, 2, 3], [(1, 3)], [[1, 2, 3], [2, 3]], [[1, 3], (1, 3)])

integrate_params = ['dark_sub_bool',
                    'polarization_factor',
                    'mask_setting',
                    'mask_dict',
                    'save_image',
                    'config_dict',]
good_kwargs = [(True, False), (.99,
                               # .95, .5
                               ),
               ('use_saved_mask_msk', 'use_saved_mask',
                'default', 'auto',
                'None',
                'array'),
               [None, {'alpha': 3}],
               (True, False), [None]]

bad_integrate_params = ['dark_sub_bool',
                        'polarization_factor',
                        'mask_setting',
                        'mask_dict',
                        'save_image',
                        'config_dict']

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
save_tiff_params = ['dark_sub_bool', 'dryrun']
save_tiff_kwarg_values = [(True, False), (True, False)]

for vs in save_tiff_kwarg_values:
    d = {k: v for (k, v) in zip(save_tiff_params, vs)}
    save_tiff_kwargs.append((d, False))

for d in [save_tiff_kwargs, integrate_kwargs]:
    for d2 in d:
        d2[0]['image_data_key'] = 'pe1_image'


@pytest.mark.parametrize(("kwargs", 'known_fail_bool'), integrate_kwargs)
def test_integrate_core_smoke(exp_db, disk_mask, fast_tmp_dir, kwargs,
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
    pprint(kwargs)
    a = integrate_and_save(exp_db[-1], db=exp_db, save_dir=fast_tmp_dir, **kwargs)
    b = integrate_and_save_last(db=exp_db, save_dir=fast_tmp_dir, **kwargs)
    if known_fail_bool and not a and not b:
        pytest.xfail('Bad params')
    if kwargs.get('dryrun', None):
        assert (
            set(old_files) != set(os.listdir(fast_tmp_dir)) and set(old_times) == set(
            [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
             os.listdir(fast_tmp_dir)]))
    else:
        assert (
            set(old_files) != set(os.listdir(fast_tmp_dir)) or set(old_times) != set(
            [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
             os.listdir(fast_tmp_dir)]))


@pytest.mark.parametrize(("kwargs", 'known_fail_bool'), save_tiff_kwargs)
def test_save_tiff_core_smoke(exp_db, fast_tmp_dir, kwargs, known_fail_bool):
    old_files = os.listdir(fast_tmp_dir)
    old_times = [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
                 os.listdir(fast_tmp_dir)]
    pprint(kwargs)
    a = save_tiff(exp_db[-1], db=exp_db, save_dir=fast_tmp_dir, **kwargs)
    b = save_last_tiff(exp_db, fast_tmp_dir, **kwargs)
    if known_fail_bool and not a and not b:
        pytest.xfail('Bad params')
    if kwargs['dryrun']:
        assert (
            set(old_files) != set(os.listdir(fast_tmp_dir)) and set(old_times) == set(
            [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
             os.listdir(fast_tmp_dir)]))
    else:
        assert (
            set(old_files) != set(os.listdir(fast_tmp_dir)) or set(old_times) != set(
            [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
             os.listdir(fast_tmp_dir)]))


# @pytest.mark.parametrize("idxs", sum_idx_values)
# def test_sum_logic_smoke(exp_db, idxs):
#     hdr = exp_db[-1]
#     event_stream = handler.exp_db.get_events(hdr, fill=True)
#
#     sub_event_streams = tee(event_stream, 2)
#     a = sum_images(sub_event_streams[0], idxs)
#     if idxs is None:
#         assert len(list(a)) == len(list(sub_event_streams[1]))
#     elif idxs is 'all':
#         assert len(list(a)) == 1
#     elif not all(isinstance(e1, list) or isinstance(e1, tuple) for e1 in
#                  idxs):
#         assert len(list(a)) == 1
#     else:
#         assert len(list(a)) == len(idxs)
