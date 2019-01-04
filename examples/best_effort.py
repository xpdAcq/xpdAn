import time
from pprint import pprint

import bluesky.plans as bp
import matplotlib.pyplot as plt
from bluesky import RunEngine
from bluesky.plan_stubs import checkpoint, abs_set, wait, trigger_and_read
from bluesky.preprocessors import pchain
from bluesky.utils import install_kicker
from bluesky.utils import short_uid
from ophyd.sim import hw, SynSignal
from rapidz import Stream
from shed.simple import (
    SimpleFromEventStream,
    SimpleToEventStream,
    AlignEventStreams,
)
from xpdan.vend.callbacks.core import StripDepVar
from xpdan.vend.callbacks.zmq import Publisher
from xpdconf.conf import glbl_dict


def one_nd_step(detectors, step, pos_cache):
    """
    Inner loop of an N-dimensional step scan

    This is the default function for ``per_step`` param in ND plans.

    Parameters
    ----------
    detectors : iterable
        devices to read
    step : dict
        mapping motors to positions in this step
    pos_cache : dict
        mapping motors to their last-set positions
    """

    def move():
        yield from checkpoint()
        grp = short_uid("set")
        for motor, pos in step.items():
            if pos == pos_cache[motor]:
                # This step does not move this motor.
                continue
            yield from abs_set(motor, pos, group=grp)
            pos_cache[motor] = pos
        yield from wait(group=grp)

    motors = step.keys()
    yield from move()
    plt.pause(.001)
    yield from trigger_and_read(list(detectors) + list(motors))


install_kicker()
p = Publisher(glbl_dict["inbound_proxy_address"])
hw = hw()
import numpy as np

rand_img = SynSignal(
    func=lambda: np.array(np.random.random((10, 10))),
    name="img",
    labels={"detectors"},
)
RE = RunEngine()
# build the pipeline
raw_source = Stream()
raw_output = SimpleFromEventStream(
    "event", ("data", "det_a"), raw_source, principle=True
)
raw_output2 = SimpleFromEventStream("event", ("data", "noisy_det"), raw_source)
raw_output3 = SimpleFromEventStream("event", ("data", "img"), raw_source)

pipeline = (
    raw_output.union(raw_output2, raw_output3.map(np.sum))
    .map(lambda x: x ** 2)
    .accumulate(lambda x, y: x + y)
)

res = SimpleToEventStream(pipeline, ("result",))

merge = AlignEventStreams(raw_source.starmap(StripDepVar()), res)
merge.sink(pprint)
# send to viz server
merge.starsink(p)

RE.subscribe(lambda *x: raw_source.emit(x))
RE.subscribe(lambda *x: p(*x))
RE.subscribe(lambda *x: time.sleep(.1))
RE.subscribe(lambda *x: time.sleep(1), "stop")

RE(
    pchain(
        bp.scan([hw.noisy_det], hw.motor, 0, 10, 10),
        bp.grid_scan(
            [hw.ab_det],
            hw.motor,
            0,
            5,
            5,
            hw.motor2,
            0,
            5,
            5,
            True,
            per_step=one_nd_step,
        ),
        bp.grid_scan(
            [hw.ab_det],
            hw.motor,
            0,
            10,
            10,
            hw.motor2,
            0,
            10,
            10,
            True,
            per_step=one_nd_step,
        ),
        bp.spiral(
            [hw.ab_det],
            hw.motor,
            hw.motor2,
            0,
            0,
            10,
            10,
            1,
            10,
            per_step=one_nd_step,
        ),
        bp.grid_scan(
            [rand_img],
            hw.motor,
            0,
            10,
            10,
            hw.motor2,
            0,
            10,
            10,
            True,
            per_step=one_nd_step,
        ),
    )
)

# raw_source.visualize('best_effort.png', source_node=True)
print("Done")
