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
from itertools import product
from pprint import pprint

import numpy as np
import pytest

from xpdan.data_reduction_core import (integrate_and_save,
                                       integrate_and_save_last,
                                       save_tiff, save_last_tiff)

sum_idx_values = (
    None, 'all', [1, 2, 3], [(1, 3)], [[1, 2, 3], [2, 3]], [[1, 3], (1, 3)])

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
def test_integrate_core_smoke(exp_db, fast_tmp_dir, kwargs,
                              known_fail_bool):
    print(kwargs)
    if known_fail_bool:
        pytest.xfail('Bad params')
    old_files = os.listdir(fast_tmp_dir)
    old_times = [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
                 os.listdir(fast_tmp_dir)]
    integrate_and_save(exp_db[-1], db=exp_db, save_dir=fast_tmp_dir, **kwargs)
    integrate_and_save_last(db=exp_db, save_dir=fast_tmp_dir, **kwargs)
    assert (set(old_files) != set(os.listdir(fast_tmp_dir)) or set(
        old_times) != set(
        [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
         os.listdir(fast_tmp_dir)]))


@pytest.mark.parametrize(("kwargs", 'known_fail_bool'), save_tiff_kwargs)
def test_save_tiff_core_smoke(exp_db, fast_tmp_dir, kwargs, known_fail_bool):
    if known_fail_bool:
        pytest.xfail('Bad params')
    old_files = os.listdir(fast_tmp_dir)
    old_times = [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
                 os.listdir(fast_tmp_dir)]
    pprint(kwargs)
    save_tiff(exp_db[-1], db=exp_db, save_dir=fast_tmp_dir, **kwargs)
    save_last_tiff(db=exp_db, save_dir=fast_tmp_dir, **kwargs)

    assert (
        set(old_files) != set(os.listdir(fast_tmp_dir)) or set(
            old_times) != set(
            [os.path.getmtime(os.path.join(fast_tmp_dir, f)) for f in
             os.listdir(fast_tmp_dir)]))
