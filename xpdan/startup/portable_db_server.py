import io
from pprint import pprint

import fire

from bluesky.utils import apply_to_dict_recursively, sanitize_np
from databroker import Broker
from rapidz import Stream
from shed.writers import NpyWriter
from xpdan.vend.callbacks.core import RunRouter, ExportCallback
from xpdan.vend.callbacks.zmq import RemoteDispatcher
from xpdconf.conf import glbl_dict
import os
import yaml

portable_template = """description: '{0} database'
metadatastore:
    module: 'databroker.headersource.sqlite'
    class: 'MDS'
    config:
        directory: '{0}'
        timezone: 'US/Eastern'
assets:
    module: 'databroker.assets.sqlite'
    class: 'Registry'
    config:
        dbpath: '{0}/assets.sqlite'
"""


def run_server(
    folder,
    outbound_proxy_address=glbl_dict["outbound_proxy_address"],
    prefix=None,
    handlers=None,
):
    """Start up the portable databroker server

    Parameters
    ----------
    folder : str
        The location where to save the portable databrokers
    outbound_proxy_address : str, optional
        The address and port of the zmq proxy. Defaults to
        ``glbl_dict["outbound_proxy_address"]``
    prefix : bytes or list of bytes, optional
        The Publisher channels to listen to. Defaults to
        ``[b"an", b"raw"]``
    """
    # TODO: convert to bytestrings if needed
    # TODO: maybe separate this into different processes?
    if prefix is None:
        prefix = [b"an", b"raw"]
    d = RemoteDispatcher(outbound_proxy_address, prefix=prefix)
    portable_folder = folder
    portable_configs = {}
    for folder_name in ["an", "raw"]:
        fn = os.path.join(portable_folder, folder_name)
        os.makedirs(fn, exist_ok=True)
        # if the path doesn't exist then make the databrokers
        with open(
            os.path.join(portable_folder, f"{folder_name}.yml"), "w"
        ) as f:
            f.write(portable_template.format(folder_name))
        print(portable_template.format(folder_name))

        print(fn)
        # TODO: add more files here, eg. a databroker readme/tutorial
        portable_configs[folder_name] = yaml.load(
            io.StringIO(portable_template.format(fn))
        )
        os.makedirs(os.path.join(fn, "data"), exist_ok=True)

    an_broker = Broker.from_config(portable_configs["an"])
    if handlers is None:
        handlers = an_broker.reg.handler_reg

    an_source = Stream()
    zed = an_source.Store(
        os.path.join(
            portable_configs["an"]["metadatastore"]["config"]["directory"],
            "data",
        ),
        NpyWriter,
    )
    zed.starsink(an_broker.insert)

    raw_broker = Broker.from_config(portable_configs["raw"])

    raw_source = Stream()
    raw_source.starmap(
        ExportCallback(
            os.path.join(
                portable_configs["raw"]["metadatastore"]["config"][
                    "directory"
                ],
                "data",
            ),
            handler_reg=handlers,
        )
    ).starsink(raw_broker.insert)

    rr = RunRouter(
        [
            lambda x: lambda *nd: raw_source.emit(nd)
            if x.get("analysis_stage", "") == "raw"
            else None
        ]
        + [
            lambda x: lambda *nd: an_source.emit(nd)
            if x.get("analysis_stage", None) == k
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

    print("Starting Portable DB Server")
    d.start()


def run_main():
    fire.Fire(run_server)


if __name__ == "__main__":
    run_main()
