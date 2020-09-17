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
from xpdan.startup.simple_stream_server import simple_stream_server, simple_stream


def interrupt(delay: float) -> None:
    time.sleep(delay)
    print("Keyboard interrupt ...")
    os.kill(os.getpid(), signal.SIGINT)


def experiment(delay: float, address: str):
    time.sleep(delay)
    RE = RunEngine()
    devices = hw()
    publisher = Publisher(address, prefix=b"raw")
    RE.subscribe(publisher)
    RE(plans.count([devices.det1]))
    return


def test_simple_server(proxy):
    process = Process(target=experiment, args=(1, proxy[0]), daemon=True)
    thread = Thread(target=interrupt, args=(3,))
    source = simple_stream()
    server = simple_stream_server(source, proxy[1], prefix=b"raw")
    thread.start()
    process.start()
    run_server(server)
    thread.join()
    process.join()
