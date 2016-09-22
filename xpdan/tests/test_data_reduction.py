from xpdan.data_reduction import integrate


def test_integrate_smoke(exp_db):
    integrate(exp_db[-1])
