import os

from ophyd.sim import NumpySeqHandler

import bluesky.plans as bp
from xpdan.callbacks import SaveBaseClass, SaveTiff


def test_SaveBaseClass(RE, hw, tmpdir):
    sbc = SaveBaseClass(
        "{base_folder}/{folder_prefix}/{start[hello]}{"
        "__independent_vars__}",
        handler_reg={},
        base_folders=tmpdir.strpath,
    )
    L = []
    RE.subscribe(lambda *x: L.append(x))
    RE(
        bp.scan(
            [hw.direct_img],
            hw.motor,
            0,
            10,
            1,
            md={
                "hello": "world",
                "folder_tag_list": ["a", "b", "c"],
                "a": "a",
                "b": "b",
                "c": "c",
            },
        )
    )

    name_param = {
        "start": (
            "start_template",
            "{base_folder}/a/b/c//world{__independent_vars__}",
        ),
        "event": (
            "filenames",
            [f"{tmpdir.strpath}/a/b/c//world_motor_0,000_arb_"],
        ),
    }

    for n, d in L:
        sbc(n, d)
        key = name_param.get(n, "")
        if key:
            assert getattr(sbc, key[0], "") == name_param[n][1]


def test_SaveTiff(RE, hw, tmpdir):
    sbc = SaveTiff(
        handler_reg={"NPY_SEQ": NumpySeqHandler},
        template="{base_folder}/{folder_prefix}/{start[hello]}{"
                 "__independent_vars__}{ext}",
        base_folders=tmpdir.strpath,
    )
    L = []
    RE.subscribe(lambda *x: L.append(x))
    RE(
        bp.scan(
            [hw.img],
            hw.motor,
            0,
            10,
            1,
            md={
                "hello": "world",
                "folder_tag_list": ["a", "b", "c"],
                "a": "a",
                "b": "b",
                "c": "c",
            },
        )
    )

    for n, d in L:
        sbc(n, d)
        if n == "event":
            assert os.path.exists(
                tmpdir.strpath + "/a/b/c//world_motor_0,000_arb_img.tiff"
            )
