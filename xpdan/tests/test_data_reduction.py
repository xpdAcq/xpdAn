from xpdan.data_reduction import integrate_and_save, sum_images
from itertools import tee

def test_integrate_smoke(exp_db, handler):
    # no auto_dark
    integrate_and_save(exp_db[-1], dark_sub_bool=False, handler=handler)
    # include auto_dark
    integrate_and_save(exp_db[-1], dark_sub_bool=True, handler=handler)


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

