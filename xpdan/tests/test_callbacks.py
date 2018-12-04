from xpdan.callbacks import RunRouter
import bluesky.plans as bp


def test_run_router(RE, hw):
    L = []
    LL = []

    def appender(start_doc):
        L.append(('start', start_doc))
        return lambda n, d: L.append((n, d))

    def not_interested(start_doc):
        return

    # Run with not interesteds
    rr = RunRouter([not_interested, ])
    rr_token = RE.subscribe(rr)

    RE(bp.count([hw.img], 1))

    # now we're interested
    RE.unsubscribe(rr_token)
    rr = RunRouter([appender, not_interested])
    rr_token = RE.subscribe(rr)
    RE.subscribe(lambda n, d: LL.append((n, d)))
    RE(bp.count([hw.img], 1))


    assert L == LL
