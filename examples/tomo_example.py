import os
import time

import bluesky.plans as bp
import dxchange
import numpy as np
import tomopy
from bluesky.run_engine import RunEngine
from ophyd.sim import SynSignal, hw
from xpdan.vend.callbacks.zmq import Publisher
from xpdconf.conf import glbl_dict

hw = hw()
fname = os.path.expanduser("~/Downloads/tooth.h5")

proj, flat, dark, theta = dxchange.read_aps_32id(fname, sino=(0, 1))

proj = tomopy.normalize(proj, flat, dark)

rot_center = tomopy.find_center(proj, theta, init=290, ind=0, tol=0.5)
# proj2 = np.hstack((proj[:, :, :],) * 200)
# proj2 = np.hstack((proj[:, :, :],) * 1)
# rot_center -= 200
proj2 = proj
m = hw.motor1
m.kind = "hinted"
mm = hw.motor2
mm.kind = "hinted"


class FullField:
    def __call__(self, *args, **kwargs):
        v = m.get()[0]
        out = proj2[int(v), :, :]
        print(v)
        time.sleep(.5)
        return out


class Pencil:
    def __call__(self, *args, **kwargs):
        v = m.get()[0]
        vv = mm.get()[0]
        out = proj2[int(v), :, int(vv)]
        print(v, vv)
        time.sleep(.1)
        return np.squeeze(out)


f = FullField()
det = SynSignal(f, name="img", labels={"detectors"})
det.kind = "hinted"

g = Pencil()
det2 = SynSignal(g, name="img", labels={"detectors"})
det2.kind = "hinted"

RE = RunEngine()
p = Publisher(glbl_dict["inbound_proxy_address"], prefix=b"raw")
t = RE.subscribe(p)
# RE.subscribe(print)
# Build scan
l = [0, 90]
for i in range(8):
    ll = l.copy()
    interval = sorted(set(ll))[1] / 2
    for lll in ll:
        j = lll + interval
        j = round(j, 0)
        if j not in l and j < 180:
            l.append(j)
# Run Full Field Scans, each scan has more slices, showing how we can minimize
# the number of slices by interleaving them by half
for i in [2 ** n for n in range(2, 8)] + [180]:
    RE(
        bp.list_scan(
            [det],
            m,
            l[:i],
            md={
                "tomo": {
                    "type": "full_field",
                    "rotation": "motor1",
                    "center": rot_center,
                }
            },
        )
    )
    print(i)
    time.sleep(3)
# Run in pencil beam geometry (this takes a long time!)
RE(
    bp.grid_scan(
        [det2],
        m,
        0,
        180,
        181,
        mm,
        0,
        639,
        640,
        True,
        md={
            "tomo": {
                "type": "pencil",
                "rotation": "motor1",
                "translation": "motor2",
                "center": rot_center,
            }
        },
    )
)
RE.abort()
