import multiprocess
import os
import signal
import threading
import time

import matplotlib.pyplot as plt
import numpy as np
import pytest
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
from xpdan.startup.tomo_server import run_server as tomo_run_server
from xpdan.startup.intensity_server import run_server as intensity_run_server
from xpdan.startup.peak_server import run_server as peak_run_server
from bluesky.callbacks.core import Retrieve
from bluesky.callbacks.zmq import Publisher


@pytest.mark.skip
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
    exp_proc = multiprocess.Process(target=run_exp, args=(2,), daemon=True)
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


@pytest.mark.skip
@pytest.mark.parametrize("save_folder", [None, True])
def test_viz_run_server(tmpdir, proxy, RE, hw, save_folder):
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
    exp_proc = multiprocess.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    try:
        print("running server")
        if save_folder:
            viz_run_server(
                handlers={"NPY_SEQ": NumpySeqHandler}, save_folder=tmpdir
            )
        else:
            viz_run_server(handlers={"NPY_SEQ": NumpySeqHandler})

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()

    # make certain we opened some figs
    assert plt.get_fignums()
    if save_folder:
        assert len(os.listdir(tmpdir)) == 2


@pytest.mark.skip
@pytest.mark.parametrize("stage_blacklist", [(), ("mask",)])
def test_analysis_run_server(tmpdir, proxy, RE, hw, stage_blacklist):
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
    exp_proc = multiprocess.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    L = []
    try:
        print("running server")
        analysis_run_server(
            diffraction_dets=["img"],
            _publisher=lambda *x: L.append(x),
            stage_blacklist=stage_blacklist,
        )

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()
    if stage_blacklist:
        assert not [
            d
            for n, d in L
            if n == "start" and d["analysis_stage"] in stage_blacklist
        ]
    starts = [doc for name, doc in L if name == 'start']
    for s in starts:
        assert "original_start_time" in s


@pytest.mark.skip
@pytest.mark.parametrize("stage_blacklist", [(), ("normalized_img",)])
def test_analysis_run_server_radiogram(
    tmpdir, proxy, RE, hw, db, stage_blacklist
):
    def delayed_sigint(delay):  # pragma: no cover
        time.sleep(delay)
        print("killing")
        os.kill(os.getpid(), signal.SIGINT)

    def run_exp(delay):  # pragma: no cover
        time.sleep(delay)
        print("running exp")

        p = Publisher(proxy[0], prefix=b"raw")
        RE.subscribe(p)
        RE.subscribe(db.insert)
        dark, = RE(bp.count([hw.img], md=dict(analysis_stage="raw")))
        flat, = RE(bp.count([hw.img], md=dict(analysis_stage="raw")))
        RE(
            bp.count(
                [hw.img],
                md=dict(
                    analysis_stage="raw",
                    sc_dk_field_uid=dark,
                    sc_flat_field_uid=flat,
                ),
            )
        )

    # Run experiment in another process (after delay)
    exp_proc = multiprocess.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    L = []
    try:
        print("running server")
        analysis_run_server(
            db=db,
            radiogram_dets=["img"],
            _publisher=lambda *x: L.append(x),
            stage_blacklist=stage_blacklist,
        )

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()
    assert L
    if stage_blacklist:
        assert not [
            d
            for n, d in L
            if n == "start" and d["analysis_stage"] in stage_blacklist
        ]


@pytest.mark.skip
def test_db_run_server(tmpdir, proxy, RE, hw, db):
    db.reg.handler_reg = {"NPY_SEQ": NumpySeqHandler}
    glbl_dict["an_db"] = db
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
    exp_proc = multiprocess.Process(target=run_exp, args=(2,), daemon=True)
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
    assert db[-1].start["analysis_stage"] == "pdf"


@pytest.mark.skip
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
        det = SynSignal(func=lambda: np.ones(10), name="gr")
        RE(bp.count([det], md=dict(analysis_stage="raw")))
        RE(bp.count([det], md=dict(analysis_stage="pdf")))

    # Run experiment in another process (after delay)
    exp_proc = multiprocess.Process(target=run_exp, args=(2,), daemon=True)
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


@pytest.mark.skip
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

        RE(
            bp.grid_scan(
                [hw.noisy_det],
                hw.motor1,
                0,
                2,
                2,
                hw.motor2,
                0,
                2,
                2,
                True,
                md={
                    "tomo": {
                        "type": "pencil",
                        "rotation": "motor1",
                        "translation": "motor2",
                        "center": 1,
                    }
                },
            )
        )

    # Run experiment in another process (after delay)
    exp_proc = multiprocess.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    L = []
    try:
        print("running server")
        tomo_run_server(_publisher=lambda *x: L.append(x), algorithm="fbp")

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()
    assert L


@pytest.mark.skip
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

        RE(
            bp.grid_scan(
                [hw.noisy_det],
                hw.motor3,
                0,
                2,
                2,
                hw.motor1,
                0,
                2,
                2,
                True,
                hw.motor2,
                0,
                2,
                2,
                True,
                md={
                    "tomo": {
                        "type": "pencil",
                        "rotation": "motor1",
                        "translation": "motor2",
                        "stack": "motor3",
                        "center": 1,
                    }
                },
            )
        )

    # Run experiment in another process (after delay)
    exp_proc = multiprocess.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    L = []
    try:
        print("running server")
        tomo_run_server(_publisher=lambda *x: L.append(x), algorithm="fbp")

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()
    assert L


@pytest.mark.skip
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

        det = SynSignal(func=lambda: np.ones((10, 10)), name="gr")
        RE(
            bp.scan(
                [det],
                hw.motor1,
                0,
                2,
                2,
                md={
                    "tomo": {
                        "type": "full_field",
                        "rotation": "motor1",
                        "center": 1,
                    }
                },
            )
        )

    # Run experiment in another process (after delay)
    exp_proc = multiprocess.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    L = []
    try:
        print("running server")
        tomo_run_server(_publisher=lambda *x: L.append(x), algorithm="fbp")

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()
    assert L


@pytest.mark.skip
def test_intensity_run_server(tmpdir, proxy, RE, hw):
    def delayed_sigint(delay):  # pragma: no cover
        time.sleep(delay)
        print("killing")
        os.kill(os.getpid(), signal.SIGINT)

    def run_exp(delay):  # pragma: no cover
        time.sleep(delay)
        print("running exp")

        p = Publisher(proxy[0], prefix=b"raw")
        RE.subscribe(p)
        z = np.zeros(10)
        z[3] = 1
        x = SynSignal(func=lambda: np.arange(10), name="x")
        y = SynSignal(func=lambda: z, name="y")
        RE(bp.count([x, y], md=dict(analysis_stage="raw")))

    # Run experiment in another process (after delay)
    exp_proc = multiprocess.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    L = []
    try:
        print("running server")
        intensity_run_server(
            positions=[3], stage="raw", x_name='x', y_name='y',
            _publisher=lambda *x: L.append(x)
        )

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()
    # assert L


@pytest.mark.skip
def test_peak_run_server(tmpdir, proxy, RE, hw):
    def delayed_sigint(delay):  # pragma: no cover
        time.sleep(delay)
        print("killing")
        os.kill(os.getpid(), signal.SIGINT)

    def run_exp(delay):  # pragma: no cover
        time.sleep(delay)
        print("running exp")

        p = Publisher(proxy[0], prefix=b"raw")
        RE.subscribe(p)
        z = np.zeros(10)
        z[3] = 1
        z[4] = 2
        z[5] = 1
        x = SynSignal(func=lambda: np.arange(10), name="x")
        y = SynSignal(func=lambda: z, name="y")
        RE(bp.count([x, y], md=dict(analysis_stage="raw")))

    # Run experiment in another process (after delay)
    exp_proc = multiprocess.Process(target=run_exp, args=(2,), daemon=True)
    exp_proc.start()

    # send the message that will eventually kick us out of the server loop
    threading.Thread(target=delayed_sigint, args=(10,)).start()
    L = []
    try:
        print("running server")
        peak_run_server(
            x_ranges=[3, 7], x_name='x', y_name='y',
            stage="raw", _publisher=lambda *x: L.append(x)
        )

    except KeyboardInterrupt:
        print("finished server")
    exp_proc.terminate()
    exp_proc.join()
    assert L
