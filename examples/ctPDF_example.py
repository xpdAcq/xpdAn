import os
import time

import bluesky.plans as bp
import dxchange
import numpy as np
import tomopy
from bluesky.run_engine import RunEngine
from ophyd.sim import SynSignal, hw
from bluesky.callbacks.zmq import Publisher
from xpdconf.conf import glbl_dict

hw = hw()
rot_center = 290
m = hw.motor1
m.kind = "hinted"
mm = hw.motor2
mm.kind = "hinted"
mmm = hw.motor3
mmm.kind = "hinted"
xrun(0,
    bp.grid_scan(
        [xpd_pe1c],
        mmm,
        0,
        2,
        2,
        m,
        0,
        180,
        4,
        True,
        mm,
        200,
        401,
        4,
        True,

        md={
            "tomo": {
                "type": "pencil",
                "rotation": "motor1",
                "translation": "motor2",
                "stack": "motor3",
                "center": rot_center - 200,
            }
        },
    )
)
