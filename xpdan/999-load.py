import os
import sys
from PyQt4 import QtGui
app = QtGui.QApplication(sys.argv)


# set env_var
# internal code: 0 -> beamline, 1 -> test, 2 -> simulation
os.environ['ENV_VAR'] = str(2)

from xpdan.qt_gui import XpdanSearch, an
from xpdan.data_reduction import *
