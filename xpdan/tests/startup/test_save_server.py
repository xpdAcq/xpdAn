import os

from event_model import RunRouter
from ophyd.sim import NumpySeqHandler

import bluesky.plans as bp
from xpdan.dev_utils import _timestampstr
from xpdan.startup.save_server import setup_saver


def test_save_server(RE, hw, tmpdir):
    L = []
    RE.subscribe(lambda *x: L.append(x))
    RE.subscribe(
        RunRouter(
            [setup_saver],
            base_folders=tmpdir.strpath,
            template="{base_folder}/{folder_prefix}/"
                     "{start[analysis_stage]}/"
                     "{start[sample_name]}_"
                     "{human_timestamp}_"
                     "{__independent_vars__}"
                     "{start[uid]:.6}_"
                     "{event[seq_num]:04d}{ext}",
            handler_reg={"NPY_SEQ": NumpySeqHandler},
        )
    )
    RE(
        bp.scan(
            [hw.img],
            hw.motor,
            0,
            10,
            1,
            md={
                "sample_name": "world",
                "folder_tag_list": ["a", "b", "c"],
                "a": "a",
                "b": "b",
                "c": "c",
                "analysis_stage": "dark_sub",
            },
        )
    )

    for n, d in L:
        if n == "event":
            start = L[0][1]
            s = f"/a/b/c//{start['analysis_stage']}/world_{_timestampstr(start['time'])}_motor_0,000_arb_{start['uid']:.6}_{d['seq_num']:04d}_img.tiff"
            assert os.path.exists(tmpdir.strpath + s)
