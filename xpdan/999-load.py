import os
import sys
from mock import MagicMock
from PyQt4 import QtGui
app = QtGui.QApplication(sys.argv)

# expected code
# 0 -> beamline
# 1 -> test
# 2 -> simulation
os.environ['ENV_VAR'] = str(2)

# setup glbl
from xpdan.glbl import an_glbl

# import functionality
from xpdan.qt_gui import XpdanSearch, an
from xpdan.data_reduction import *

db = an_glbl.exp_db # alias
