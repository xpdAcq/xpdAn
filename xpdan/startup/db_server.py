import io

from databroker import Broker
from rapidz import Stream
from xpdan.vend.callbacks.core import RunRouter, ExportCallback
from xpdan.vend.callbacks.zmq import RemoteDispatcher
from xpdconf.conf import glbl_dict
import os
import yaml


d = RemoteDispatcher(
    glbl_dict["outbound_proxy_address"], prefix=[b"clean_an", b"raw"]
)

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

if __name__ == "__main__":
    # canon_an_db = Broker.named(glbl_dict["an_db"])

    # TODO: get target folders somewhere
    import tempfile

    tmp_folder = tempfile.TemporaryDirectory()
    portable_folder = tmp_folder.name
    portable_configs = {}
    for folder_name in ["an", "raw"]:
        fn = os.path.join(portable_folder, folder_name)
        # if the path doesn't exist then make the databrokers
        with open(os.path.join(portable_folder, f"{folder_name}.yml"), "w") as f:
            f.write(portable_template.format(folder_name))
        print(portable_template.format(fn))

        if not os.path.exists(fn):
            os.makedirs(fn)
            # TODO: add more files here, eg. a databroker readme/tutorial
            portable_configs[folder_name] = yaml.load(
                io.StringIO(portable_template.format(fn))
            )
            os.makedirs(os.path.join(fn, "data"), exist_ok=True)

    an_broker = Broker.from_config(portable_configs["an"])

    an_source = Stream()
    an_source.starmap(
        ExportCallback(
            os.path.join(
                portable_configs["an"]["metadatastore"]["config"]["directory"],
                "data",
            ),
            an_broker.reg.handler_reg,
        )
    ).starsink(an_broker.insert)
    """
    # Uncomment for beamline config 

    an_source.starmap(
        ExportCallback(
            os.path.join(
                # FIXME: path to analysis data storage goes here!
                '...',
                "data",
            ),
            an_broker.reg.handler_reg,
        )
    ).starsink(canon_an_db.insert)
    """

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
            raw_broker.reg.handler_reg,
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
    try:
        d.start()
    finally:
        tmp_folder.cleanup()
