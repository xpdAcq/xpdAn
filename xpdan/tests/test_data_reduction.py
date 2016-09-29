from xpdan.data_reduction import integrate_and_save, sum_images
from itertools import tee, product
import pytest
from pprint import pprint

sum_idx_values = (
    None, 'all', [1, 2, 3], [(1, 3)], [[1, 2, 3], [2, 3]], [[1, 3], (1, 3)])

integrate_params = ['dark_sub_bool',
                    'polarization_factor',
                    'auto_mask',
                    'mask_dict',
                    'save_image',
                    'root_dir',
                    'config_dict',
                    'sum_idx_list']
dsb = (True, False)
pf = (.99, .95, .5)
am = (True, False)
md = [None]
si = (True, False)
rd = [None]
cd = [None]

integrate_kwarg_values = product(dsb, pf, am, md, si, rd, cd, sum_idx_values)
integrate_kwargs = []
for vs in integrate_kwarg_values:
    d = {k: v for (k, v) in zip(integrate_params, vs)}
    integrate_kwargs.append(d)


@pytest.mark.parametrize("kwargs", integrate_kwargs)
def test_integrate_smoke(exp_db, handler, kwargs):
    pprint(kwargs)
    integrate_and_save(exp_db[-1], handler=handler, **kwargs)


def test_sum_logic_smoke(exp_db, handler):
    hdr = exp_db[-1]
    event_stream = handler.exp_db.get_events(hdr, fill=True)
    sub_event_streams = tee(event_stream, 4)
    a = sum_images(sub_event_streams[0])
    assert len(list(a)) == len(list(sub_event_streams[1]))
    a = sum_images(sub_event_streams[2], [1, 2, 3])
    assert len(list(a)) == 1
    a = sum_images(sub_event_streams[3], [(1, 3)])
    assert len(list(a)) == 1
