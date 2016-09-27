import os
import sys
from mock import MagicMock
from PyQt4 import QtGui
app = QtGui.QApplication(sys.argv)

import matplotlib
matplotlib.use('qt4agg')
import tempfile
import os
import tzlocal
from bluesky import RunEngine
from bluesky.plans import scan, count, relative_scan
from bluesky.examples import Reader, motor

# set env_var
# expected internal code: 
# 0 -> beamline
# 1 -> test
# 2 -> simulation
os.environ['ENV_VAR'] = str(2)

from xpdan.glbl import an_glbl

# make db
def make_broker():
    from portable_mds.sqlite.mds import MDS
    from portable_fs.sqlite.fs import FileStore
    from databroker import Broker

    # make visible temp_dir, a layer up than xpdUser
    mds_dir = os.path.join(an_glbl.base, tempfile.mkdtemp())
    mds = MDS({'directory': mds_dir,
               'timezone': tzlocal.get_localzone().zone})
    fs = FileStore({'dbpath': os.path.join(mds_dir, 'filestore.db')})
    return Broker(mds, fs)


class SimulatedPE1C(Reader):
    """Subclass the bluesky plain detector examples ('Reader'); add attributes."""

    def __init__(self, name, read_fields):
        self.images_per_set = MagicMock()
        self.images_per_set.get = MagicMock(return_value=5)
        self.number_of_sets = MagicMock()
        self.number_of_sets.put = MagicMock(return_value=1)
        self.number_of_sets.get = MagicMock(return_value=1)
        self.cam = MagicMock()
        self.cam.acquire_time = MagicMock()
        self.cam.acquire_time.put = MagicMock(return_value=0.1)
        self.cam.acquire_time.get = MagicMock(return_value=0.1)
        self._staged = False

        super().__init__(name, read_fields)

        self.ready = True  # work around a hack in Reader

    def stage(self):
        if self._staged:
            raise RuntimeError("Device is already staged.")
        self._staged = True
        return [self]

    def unstage(self):
        self._staged = False


if os.environ['ENV_VAR'] == str(2):
    # simulated db
    db = make_broker()
    # simulated det
    pe1c = SimulatedPE1C('pe1c',
                         {'pe1_image': lambda: np.random.randn(25,25)})

    # TODO : add md schema later
    RE = RunEngine({})
    RE.subscribe('all', db.mds.insert)

    def generate_simulation_data():
        # This adds {'proposal_id': 1} to all future runs, unless overridden.
        RE(count([pe1c]))
        RE(scan([pe1c], motor, 1, 5, 5))
        RE(scan([pe1c], motor, 1, 10, 10))


# db alias, at beamline, real db is handed
an_glbl.db = db

# import functionality
from xpdan.qt_gui import XpdanSearch, an
from xpdan.data_reduction import *
