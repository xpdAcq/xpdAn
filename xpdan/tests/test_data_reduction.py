from xpdan.data_reduction import integrate


def test_integrate_smoke(exp_db, handler):
    integrate(exp_db[-1], handler=handler)
