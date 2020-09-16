"""Replay example XPD data into ZMQ proxy"""
import copy
import time

from pprint import pprint
from databroker.broker import Broker
from bluesky.callbacks.zmq import Publisher

# pull from local data, not needed at beamline
db = Broker.named("live_demo_data")
db.prepare_hook = lambda x, y: copy.deepcopy(y)

from xpdconf.conf import glbl_dict

p = Publisher(
    glbl_dict["inbound_proxy_address"], prefix=b"raw",
)

stopped = False
t0 = time.time()
try:
    for hdr in list((db[-1],)):
        stop = hdr.stop
        start = hdr.start
        for e in hdr.documents():
            if e[0] == "start":
                e[1].update(composition_string="EuTiO3")
                e[1].update(
                    hints={
                        "dimensions": [
                            (
                                ["temperature", "temperature_setpoint"],
                                "primary",
                            )
                        ]
                    }
                )
                e[1].update(analysis_stage="raw")
            if e[0] == "descriptor":
                e[1].update(
                    hints={
                        "cs700": {
                            "fields": ["temperature", "temperature_setpoint"]
                        }
                    }
                )
            # if e[1]["seq_num"] > 3:
            #     break
            if e[0] == "resource":
                if "run_start" not in e[1]:
                    e[1].update(run_start=start["uid"])
            if e[0] == "stop":
                stopped = True
            print(e[0])
            pprint(e[1])
            p(*e)
            #if e[0] == "event":
                #input()
            input()
            # time.sleep(1)
        # e = ("stop", stop)
        # p(*e)
        # time.sleep(1)
finally:
    if not stopped:
        print('stop')
        pprint(stop)
        p("stop", stop)

print(time.time() - t0)
