from xpdan.db_utils import sort_scans_by_hdr_key, scan_diff, scan_summary


def test_sort_scans_by_hdr_key(exp_db):
    hdrs = exp_db()
    print(hdrs[0].start)
    d = sort_scans_by_hdr_key(hdrs, 'pi_name')
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
    assert 'uid' not in d
    return


def test_scan_summary(exp_db):
    hdrs = exp_db()
    d = scan_summary(hdrs)
    assert len(d) != 0
