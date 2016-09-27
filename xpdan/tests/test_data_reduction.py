from xpdan.data_reduction import integrate, sum_images


def test_integrate_smoke(exp_db, handler):
    integrate(exp_db[-1], handler=handler)


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
