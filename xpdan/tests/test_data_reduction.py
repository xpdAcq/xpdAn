from xpdan.data_reduction import integrate_and_save, sum_images, \
    integrate_and_save_last, save_tiff, save_last_tiff
from itertools import tee, product
import pytest
from pprint import pprint
import numpy as np
import os

sum_idx_values = (
    None, 'all', [1, 2, 3], [(1, 3)], [[1, 2, 3], [2, 3]], [[1, 3], (1, 3)])

integrate_params = ['dark_sub_bool',
                    'polarization_factor',
                    'mask_setting',
                    'mask_dict',
                    'save_image',
                    'root_dir',
                    'config_dict',
                    'sum_idx_list']
good_kwargs = [(True, False), (.99,
                               # .95, .5
                               ), ('use_saved_mask', 'default', 'auto','None',
                                   np.random.random_integers(
                                       0, 1, (200, 200)).astype(bool)),
                               [None, {'alpha': 3}],
               (True, False), [None], [None], sum_idx_values]

bad_integrate_params = ['dark_sub_bool',
                        'polarization_factor',
                        'mask_setting',
                        'mask_dict',
                        'save_image',
                        'config_dict',
                        'sum_idx_list']

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
save_tiff_params = ['dark_sub_bool', 'max_count', 'dryrun']
save_tiff_kwarg_values = [(True, False), (None, 1), (True, False)]

for vs in save_tiff_kwarg_values:
    d = {k: v for (k, v) in zip(save_tiff_params, vs)}
    save_tiff_kwargs.append((d, False))


@pytest.mark.parametrize(("kwargs", 'known_fail_bool'), integrate_kwargs)
def test_integrate_smoke(exp_db, handler, disk_mask, kwargs, known_fail_bool):
    if 'mask_setting' in kwargs.keys() and \
                    kwargs['mask_setting'] == 'use_saved_mask':
        kwargs['mask_setting'] = disk_mask[0]
    pprint(kwargs)
    a = integrate_and_save(exp_db[-1], handler=handler, **kwargs)
    if known_fail_bool and not a:
        pytest.xfail('Bad params')


@pytest.mark.parametrize(("kwargs", 'known_fail_bool'), integrate_kwargs)
def test_integrate_and_save_last_smoke(handler, disk_mask, kwargs,
                                       known_fail_bool):
    if 'mask_setting' in kwargs.keys() and \
                    kwargs['mask_setting'] == 'use_saved_mask':
        kwargs['mask_setting'] = disk_mask[0]
    pprint(kwargs)
    a = integrate_and_save_last(handler=handler, **kwargs)
    if known_fail_bool and not a:
        pytest.xfail('Bad params')


@pytest.mark.parametrize(("kwargs", 'known_fail_bool'), save_tiff_kwargs)
def test_save_tiff_smoke(exp_db, handler, kwargs, known_fail_bool):
    pprint(kwargs)
    a = save_tiff(exp_db[-1], handler=handler, **kwargs)
    if known_fail_bool and not a:
        pytest.xfail('Bad params')


@pytest.mark.parametrize(("kwargs", 'known_fail_bool'), save_tiff_kwargs)
def test_save_last_tiff_smoke(handler, kwargs, known_fail_bool):
    pprint(kwargs)
    a = save_last_tiff(handler=handler, **kwargs)
    if known_fail_bool and not a:
        pytest.xfail('Bad params')


@pytest.mark.parametrize("idxs", sum_idx_values)
def test_sum_logic_smoke(exp_db, handler, idxs):
    hdr = exp_db[-1]
    event_stream = handler.exp_db.get_events(hdr, fill=True)

    sub_event_streams = tee(event_stream, 2)
    a = sum_images(sub_event_streams[0], idxs)
    if idxs is None:
        assert len(list(a)) == len(list(sub_event_streams[1]))
    elif idxs is 'all':
        assert len(list(a)) == 1
    elif not all(isinstance(e1, list) or isinstance(e1, tuple) for e1 in
                 idxs):
        assert len(list(a)) == 1
    else:
        assert len(list(a)) == len(idxs)
