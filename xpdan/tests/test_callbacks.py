from ophyd.sim import NumpySeqHandler
from xpdan.callbacks import SaveBaseClass, SaveTiff
import bluesky.plans as bp
import os


def test_SaveBaseClass(RE, hw, tmpdir):
    sbc = SaveBaseClass(
        "{base_folder}/{folder_prefix}/{start[hello]}{"
        "__independent_vars__}",
        handler_reg={},
        base_folder=tmpdir.strpath,
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
            "/a/b/c//world_motor_{event[data][motor]:1.{descriptor["
            "data_keys][motor][precision]}f}_{descriptor[data_keys][motor]["
            "units]}_",
        ),
        "event": (
            "filename",
            "/a/b/c//world_motor_0,000_{descriptor[data_keys][motor]["
            "units]}_",
        ),
    }

    for n, d in L:
        sbc(n, d)
        key = name_param.get(n, "")
        if key:
            assert (
                getattr(sbc, key[0], "") == tmpdir.strpath + name_param[n][1]
            )


def test_SaveTiff(RE, hw, tmpdir):
    sbc = SaveTiff(
        handler_reg={"NPY_SEQ": NumpySeqHandler},
        template="{base_folder}/{folder_prefix}/{start[hello]}{"
        "__independent_vars__}{ext}",
        base_folder=tmpdir.strpath,
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
            print(sbc.filename)
            assert os.path.exists(
                tmpdir.strpath + "/a/b/c//world_motor_0,000_img.tiff"
            )
