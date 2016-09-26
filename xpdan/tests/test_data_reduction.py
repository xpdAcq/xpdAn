from xpdan.data_reduction import integrate, sum_images


def test_integrate_smoke(exp_db, handler):
    integrate(exp_db[-1], handler=handler)


def test_sum_logic_smoke(exp_db, handler):
    a = sum_images(exp_db[-1])
    assert len(a) == 1
    a = sum_images(exp_db[-1], [1, 2, 3])
    assert len(a) == 1
    a = sum_images(exp_db[-1], [(1, 3)])
    assert len(a) == 1
