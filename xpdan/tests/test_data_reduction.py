from xpdan.data_reduction import integrate_and_save, sum_images


def test_integrate_smoke(exp_db, handler):
    # no auto_dark
    integrate_and_save(exp_db[-1], dark_sub=False, handler=handler)
    # include auto_dark
    integrate_and_save(exp_db[-1], dark_sub=True, handler=handler)


def test_sum_logic_smoke(exp_db, handler):
    print(exp_db)
    hdr = exp_db[-1]
    print(hdr)
    a = sum_images(hdr, handler=handler)
    assert len(a) == 1
    a = sum_images(hdr, [1, 2, 3], handler=handler)
    assert len(a) == 1
    a = sum_images(hdr, [(1, 3)], handler=handler)
    assert len(a) == 1

