import multiprocessing
import os
import signal
import threading
import time

import matplotlib.pyplot as plt
import numpy as np
from shed.simple import SimpleFromEventStream

import bluesky.plans as bp
from ophyd.sim import NumpySeqHandler, SynSignal
from rapidz import Stream
from xpdconf.conf import glbl_dict
from xpdan.startup.portable_db_server import (
    run_server as portable_db_run_sever
)
from xpdan.startup.viz_server import run_server as viz_run_server
from xpdan.startup.analysis_server import run_server as analysis_run_server
from xpdan.startup.db_server import run_server as db_run_server
from xpdan.startup.qoi_server import run_server as qoi_run_server
from xpdan.startup.tomo_server import  run_server as tomo_run_server
from xpdan.vend.callbacks.core import Retrieve
from xpdan.vend.callbacks.zmq import Publisher


def test_portable_db_run_server(tmpdir, proxy, RE, hw):
    fn = str(tmpdir)

    def delayed_sigint(delay):  # pragma: no cover
        time.sleep(delay)
        print("killing")
        os.kill(os.getpid(), signal.SIGINT)

    def run_exp(delay):  # pragma: no cover
        time.sleep(delay)
        print("running exp")

        p = Publisher(proxy[0], prefix=b"raw")
        RE.subscribe(p)

        # Tiny fake pipeline
        pp = Publisher(proxy[0], prefix=b"an")
        raw_source = Stream()
        SimpleFromEventStream(
            "event",
            ("data", "img"),
            raw_source.starmap(Retrieve({"NPY_SEQ": NumpySeqHandler})),
            principle=True,
        ).map(lambda x: x * 2).SimpleToEventStream(
            ("img2",), analysis_stage="pdf"
        ).starsink(
            pp
        )
        RE.subscribe(lambda *x: raw_source.emit(x))

        RE(bp.count([hw.img], md=dict(analysis_stage="raw")))
        print("finished exp")
        p.close()

    # Run experiment in another process (after delay)
    exp_proc = multiprocessing.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    try:
        print("running server")
        portable_db_run_sever(fn, handlers={"NPY_SEQ": NumpySeqHandler})

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()
    for k in ["raw", "an"]:
        assert os.path.exists(os.path.join(fn, k))
        assert os.path.exists(os.path.join(fn, f"{k}.yml"))
        for kk in ["run_starts", "run_stops", "event_descriptors"]:
            assert os.path.exists(os.path.join(fn, f"{k}/{kk}.json"))


def test_viz_run_server(tmpdir, proxy, RE, hw):
    def delayed_sigint(delay):  # pragma: no cover
        time.sleep(delay)
        print("killing")
        os.kill(os.getpid(), signal.SIGINT)

    def run_exp(delay):  # pragma: no cover
        time.sleep(delay)
        print("running exp")

        p = Publisher(proxy[0], prefix=b"raw")
        RE.subscribe(p)

        # Tiny fake pipeline
        pp = Publisher(proxy[0], prefix=b"an")
        raw_source = Stream()
        SimpleFromEventStream(
            "event",
            ("data", "img"),
            raw_source.starmap(Retrieve({"NPY_SEQ": NumpySeqHandler})),
            principle=True,
        ).map(lambda x: x * 2).SimpleToEventStream(
            ("img2",), analysis_stage="pdf"
        ).starsink(
            pp
        )
        RE.subscribe(lambda *x: raw_source.emit(x))

        RE(bp.count([hw.img], md=dict(analysis_stage="raw")))
        print("finished exp")
        p.close()

    # Run experiment in another process (after delay)
    exp_proc = multiprocessing.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    try:
        print("running server")
        viz_run_server(handlers={"NPY_SEQ": NumpySeqHandler})

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()

    # make certain we opened some figs
    assert plt.get_fignums()


def test_analysis_run_server(tmpdir, proxy, RE, hw):
    def delayed_sigint(delay):  # pragma: no cover
        time.sleep(delay)
        print("killing")
        os.kill(os.getpid(), signal.SIGINT)

    def run_exp(delay):  # pragma: no cover
        time.sleep(delay)
        print("running exp")

        p = Publisher(proxy[0], prefix=b"raw")
        RE.subscribe(p)
        RE(bp.count([hw.img], md=dict(analysis_stage="raw")))

    # Run experiment in another process (after delay)
    exp_proc = multiprocessing.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    try:
        print("running server")
        analysis_run_server(diffraction_dets=['img'])

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()


def test_db_run_server(tmpdir, proxy, RE, hw, db):
    db.reg.handler_reg = {"NPY_SEQ": NumpySeqHandler}
    glbl_dict['an_db'] = db
    fn = str(tmpdir)

    def delayed_sigint(delay):  # pragma: no cover
        time.sleep(delay)
        print("killing")
        os.kill(os.getpid(), signal.SIGINT)

    def run_exp(delay):  # pragma: no cover
        time.sleep(delay)
        print("running exp")

        p = Publisher(proxy[0], prefix=b"raw")
        RE.subscribe(p)

        # Tiny fake pipeline
        pp = Publisher(proxy[0], prefix=b"an")
        raw_source = Stream()
        SimpleFromEventStream(
            "event",
            ("data", "img"),
            raw_source.starmap(Retrieve({"NPY_SEQ": NumpySeqHandler})),
            principle=True,
        ).map(lambda x: x * 2).SimpleToEventStream(
            ("img2",), analysis_stage="pdf"
        ).starsink(
            pp
        )
        RE.subscribe(lambda *x: raw_source.emit(x))

        RE(bp.count([hw.img], md=dict(analysis_stage="raw")))
        print("finished exp")
        p.close()

    # Run experiment in another process (after delay)
    exp_proc = multiprocessing.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    try:
        print("running server")
        db_run_server(fn)

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()
    assert db[-1].start['analysis_stage'] == 'pdf'


def test_qoi_run_server(tmpdir, proxy, RE, hw):
    def delayed_sigint(delay):  # pragma: no cover
        time.sleep(delay)
        print("killing")
        os.kill(os.getpid(), signal.SIGINT)

    def run_exp(delay):  # pragma: no cover
        time.sleep(delay)
        print("running exp")

        p = Publisher(proxy[0], prefix=b"raw")
        RE.subscribe(p)
        det = SynSignal(func=lambda: np.ones(10), name='gr')
        RE(bp.count([det], md=dict(analysis_stage="raw")))
        RE(bp.count([det], md=dict(analysis_stage="pdf")))

    # Run experiment in another process (after delay)
    exp_proc = multiprocessing.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    L = []
    try:
        print("running server")
        qoi_run_server(_publisher=lambda *x: L.append(x))

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()
    assert L


def test_tomo_run_server_2d_pencil(tmpdir, proxy, RE, hw):
    def delayed_sigint(delay):  # pragma: no cover
        time.sleep(delay)
        print("killing")
        os.kill(os.getpid(), signal.SIGINT)

    def run_exp(delay):  # pragma: no cover
        time.sleep(delay)
        print("running exp")

        p = Publisher(proxy[0], prefix=b"an")
        RE.subscribe(p)

        RE(bp.grid_scan([hw.noisy_det],
                   hw.motor1, 0, 2, 2,
                   hw.motor2, 0, 2, 2, True,
                   md={'tomo': {'type': 'pencil',
                                'rotation': 'motor1',
                                "translation": "motor2",
                                'center': 1}}))

    # Run experiment in another process (after delay)
    exp_proc = multiprocessing.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    L = []
    try:
        print("running server")
        tomo_run_server(_publisher=lambda *x: L.append(x))

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()
    assert L


def test_tomo_run_server_3d_pencil(tmpdir, proxy, RE, hw):
    def delayed_sigint(delay):  # pragma: no cover
        time.sleep(delay)
        print("killing")
        os.kill(os.getpid(), signal.SIGINT)

    def run_exp(delay):  # pragma: no cover
        time.sleep(delay)
        print("running exp")

        p = Publisher(proxy[0], prefix=b"an")
        RE.subscribe(p)

        RE(bp.grid_scan([hw.noisy_det],
                   hw.motor3, 0, 2, 2,
                   hw.motor1, 0, 2, 2, True,
                   hw.motor2, 0, 2, 2, True,
                   md={'tomo': {'type': 'pencil',
                                'rotation': 'motor1',
                                "translation": "motor2",
                                "stack": "motor3",
                                'center': 1}}))

    # Run experiment in another process (after delay)
    exp_proc = multiprocessing.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    L = []
    try:
        print("running server")
        tomo_run_server(_publisher=lambda *x: L.append(x))

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()
    assert L


def test_tomo_run_server_full_field(tmpdir, proxy, RE, hw):
    def delayed_sigint(delay):  # pragma: no cover
        time.sleep(delay)
        print("killing")
        os.kill(os.getpid(), signal.SIGINT)

    def run_exp(delay):  # pragma: no cover
        time.sleep(delay)
        print("running exp")

        p = Publisher(proxy[0], prefix=b"an")
        RE.subscribe(p)

        det = SynSignal(func=lambda: np.ones((10, 10)), name='gr')
        RE(bp.scan([det],
                   hw.motor1, 0, 2, 2,
                   md={'tomo': {'type': 'full_field',
                                'rotation': 'motor1',
                                'center': 1}}))

    # Run experiment in another process (after delay)
    exp_proc = multiprocessing.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    L = []
    try:
        print("running server")
        tomo_run_server(_publisher=lambda *x: L.append(x))

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()
    assert L
