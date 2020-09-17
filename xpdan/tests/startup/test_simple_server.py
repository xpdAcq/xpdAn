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
from xpdan.startup.simple_server import simple_server


def interrupt(delay: float) -> None:
    time.sleep(delay)
    print("Keyboard interrupt ...")
    os.kill(os.getpid(), signal.SIGINT)


def experiment(delay: float, address: str, prefix: str):
    time.sleep(delay)
    RE = RunEngine()
    devices = hw()
    publisher = Publisher(address, prefix=prefix)
    RE.subscribe(publisher)
    RE(plans.count([devices.img]))
    return


def test_simple_server(proxy):
    prefix = b"raw"
    process = Process(target=experiment, args=(1, proxy[0], prefix), daemon=True)
    thread = Thread(target=interrupt, args=(3,))
    server = simple_server(proxy[1], prefix=prefix)
    thread.start()
    process.start()
    run_server(server)
    thread.join()
    process.join()
