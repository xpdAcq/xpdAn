import bluesky.plans as bp
import numpy as np
from ophyd.sim import SynSignal
from xpdan.startup.tomo_server import tomo_callback_factory
from xpdan.vend.callbacks.core import RunRouter


def test_pencil_tomo_pipeline(RE, hw):
    L = []
    rr = RunRouter(
        [lambda x: tomo_callback_factory(x, publisher=lambda *x: L.append(x))]
    )
    RE.subscribe(rr)
    RE(
        bp.grid_scan(
            [hw.det1],
            hw.motor1,
            0,
            180,
            30,
            hw.motor2,
            -5,
            5,
            5,
            False,
            md={
                "tomo": {
                    "type": "pencil",
                    "translation": "motor2",
                    "rotation": "motor1",
                    "center": 0.0,
                }
            },
        )
    )
    # det1
    assert len(L) == 30 * 5 + 3


def test_full_field_tomo_pipeline(RE, hw):
    L = []
    rr = RunRouter(
        [lambda x: tomo_callback_factory(x, publisher=lambda *x: L.append(x))]
    )
    RE.subscribe(rr)
    direct_img = SynSignal(
        func=lambda: np.array(np.random.random((10, 10))),
        name="img",
        labels={"detectors"},
    )
    RE(
        bp.scan(
            [direct_img],
            hw.motor1,
            0,
            180,
            30,
            md={
                "tomo": {
                    "type": "full_field",
                    "rotation": "motor1",
                    "center": 0.0,
                }
            },
        )
    )
    # det1
    assert len(L) == 30 + 3
    print(L[-2][1]["data"]["img_tomo"].shape)
    assert len(L[4][1]["data"]["img_tomo"].shape) == 3
