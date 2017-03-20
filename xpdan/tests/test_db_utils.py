from xpdan.db_utils import *


def test_need_name_here(exp_db):
    hdrs = exp_db()
    print(hdrs[0].start)
    d = need_name_here(hdrs, 'pi_name')
    print(d)
    assert len(d) == 2
    assert 'tim' in d.keys()
    assert 'chris' in d.keys()
    return


def test_scan_diff(exp_db):
    hdrs = exp_db()
    d = scan_diff(hdrs)
    print(d)
    assert 'pi_name' in d
    return


def test_scan_headlines(exp_db):
    hdrs = exp_db()
    d = scan_headlines(hdrs)
    assert len(d) != 0

