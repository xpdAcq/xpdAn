import pytest
import os
import signal
import time
from multiprocessing import Process
from threading import Thread

from ophyd.sim import hw

import bluesky.plans as plans
from bluesky import RunEngine
from bluesky.callbacks.zmq import Publisher
from xpdan.run_server import run_server
from xpdan.startup.analysis_server import analysis_server


def interrupt(delay: float) -> None:
    time.sleep(delay)
    print("Keyboard interrupt ...")
    os.kill(os.getpid(), signal.SIGINT)


def experiment(delay: float, address: str, prefix: str):
    time.sleep(delay)
    print("Run scan ...")
    RE = RunEngine()
    devices = hw()
    publisher = Publisher(address, prefix=prefix)
    RE.subscribe(publisher)
    RE(plans.count([devices.img]), md=dict(analysis_stage="raw"))
    return


@pytest.mark.parametrize("stage_blacklist", [(), ("mask",)])
def test_analysis_server(proxy, stage_blacklist):
    prefix = b"raw"
    process = Process(target=experiment, args=(0.5, proxy[0], prefix), daemon=True)
    thread = Thread(target=interrupt, args=(8.,))
    docs = list()
    server = analysis_server(
        diffraction_dets=["img"],
        _publisher=lambda *doc: docs.append(doc),
        stage_blacklist=stage_blacklist,
    )
    process.start()
    thread.start()
    run_server(server)
    thread.join()
    process.join()
    # check
    not_skipped = [doc for name, doc in docs if name == "start" and doc["analysis_stage"] in stage_blacklist]
    if stage_blacklist:
        assert len(not_skipped) == 0
    starts = [doc for name, doc in docs if name == 'start']
    for start in starts:
        assert "original_start_time" in start
