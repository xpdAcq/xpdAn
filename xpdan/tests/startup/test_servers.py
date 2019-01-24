import multiprocessing
import os
import signal
import threading
import time

import matplotlib.pyplot as plt
from shed.simple import SimpleFromEventStream

import bluesky.plans as bp
from ophyd.sim import NumpySeqHandler
from rapidz import Stream
from xpdan.startup.portable_db_server import (
    run_server as portable_db_run_sever
)
from xpdan.startup.viz_server import run_server as viz_run_server
from xpdan.vend.callbacks.core import Retrieve
from xpdan.vend.callbacks.zmq import Publisher


def test_save_run_server(tmpdir, proxy, RE, hw):
    fn = str(tmpdir)

    def delayed_sigint(delay):
        time.sleep(delay)
        print("killing")
        os.kill(os.getpid(), signal.SIGINT)

    def run_exp(delay):
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
    def delayed_sigint(delay):
        time.sleep(delay)
        print("killing")
        os.kill(os.getpid(), signal.SIGINT)

    def run_exp(delay):
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
