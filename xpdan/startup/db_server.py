import io

import fire

from databroker import Broker
from rapidz import Stream
from shed.writers import NpyWriter
from xpdan.vend.callbacks.core import RunRouter, ExportCallback
from xpdan.vend.callbacks.zmq import RemoteDispatcher
from xpdconf.conf import glbl_dict
import os
import yaml


def run_server(
    data_dir,
    outbound_proxy_address=glbl_dict["outbound_proxy_address"],
    prefix=b"an",
):

    d = RemoteDispatcher(outbound_proxy_address, prefix=prefix)
    an_broker = glbl_dict["an_db"]

    an_source = Stream()
    an_source.Store(data_dir, NpyWriter).starsink(an_broker.insert)

    rr = RunRouter(
        [
            lambda x: lambda *nd: an_source.emit(nd)
            if x.get("analysis_stage", "") == k
            else None
            for k in [
                # TODO: put this in when we have the writers hooked up
                # 'calib',
                "integration",
                "fq",
                "pdf",
            ]
        ]
    )

    d.subscribe(rr)

    print("Starting DB Server")
    d.start()


def run_main():
    fire.Fire(run_server)


if __name__ == "__main__":
    run_main()
