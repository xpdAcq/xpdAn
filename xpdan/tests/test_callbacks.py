from xpdan.callbacks import RunRouter
import bluesky.plans as bp


def test_run_router(RE, hw):
    L = []
    LL = []

    def appender(start_doc):
        L.append(('start', start_doc))
        return lambda n, d: L.append((n, d))

    rr = RunRouter([appender])
    RE.subscribe(rr)
    RE.subscribe(lambda n, d: LL.append((n, d)))

    RE(bp.count([hw.det1], 1))

    assert L == LL
