import numpy as np
from bluesky.plan_stubs import trigger_and_read
import bluesky.plans as bp
from bluesky.plans import count
from bluesky.preprocessors import stage_decorator, run_decorator
from bluesky.tests.conftest import NumpySeqHandler
from xpdan.callbacks import Retrieve, ExportCallback
import os
from xpdan.callbacks import RunRouter


def test_retrieve(RE, hw):
    rt = Retrieve(handler_reg={'NPY_SEQ': NumpySeqHandler})
    RE.subscribe(rt)

    @stage_decorator([hw.img])
    @run_decorator()
    def plan(dets):
        data_id = yield from trigger_and_read(dets)
        data = rt.retrieve_datum(data_id['img']['value'])
        np.testing.assert_allclose(data, np.ones((10, 10)))

    RE(plan([hw.img]))


# Databroker doesn't give back resource/datums

'''
def test_retrieve_db(RE, hw, db):
    rt = Retrieve(handler_reg={'NPY_SEQ': NumpySeqHandler})
    RE.subscribe(db.insert)

    RE(count([hw.img]))

    docs = map(lambda x: rt(*x), db[-1].documents())
    for n, d in docs:
        if n == 'event':
            data = d['data']['img']
            break

    np.testing.assert_allclose(data, np.ones((10, 10)))
'''


def test_export_file(RE, hw, tmpdir):
    rt = ExportCallback(str(tmpdir), handler_reg={'NPY_SEQ': NumpySeqHandler})
    L = []
    L2 = []
    RE.subscribe(lambda n, d: L2.append((n, d)))
    RE.subscribe(lambda n, d: L.append(rt(n, d)))

    RE(count([hw.img], 1))
    assert len(os.listdir(str(tmpdir))) == 1
    assert L[2][1]['root'] == str(tmpdir)
    assert L2[2][1]['root'] != L[2][1]['root']


def test_export(RE, hw, tmpdir, db):
    rt = ExportCallback(str(tmpdir), handler_reg={'NPY_SEQ': NumpySeqHandler})
    RE.subscribe(lambda n, d: db.insert(*rt(n, d)))

    RE(count([hw.img], 1))
    for (n, d), (n2, d2) in zip(db[-1].documents(fill=True),
                                db[-1].documents()):
        if n == 'event':
            data = d['data']['img']
            np.testing.assert_allclose(data, np.ones((10, 10)))
            assert d['data']['img'] != d2['data']['img']


def test_run_router(RE, hw):
    L = []
    LL = []

    def appender(start_doc):
        L.append(("start", start_doc))
        return lambda n, d: L.append((n, d))

    def not_interested(start_doc):
        return

    # Run we're not interested in
    rr = RunRouter([not_interested])
    rr_token = RE.subscribe(rr)

    RE(bp.count([hw.img], 1))

    # now we're interested
    rr.callback_factories.append(appender)
    RE.subscribe(lambda n, d: LL.append((n, d)))
    RE(bp.count([hw.img], 1))

    assert L == LL
