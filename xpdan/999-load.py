##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Timothy Liu, Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
import os
import sys
from mock import MagicMock
from PyQt4 import QtGui
app = QtGui.QApplication(sys.argv)

# expected code
# 0 -> beamline
# 1 -> test
# 2 -> simulation
os.environ['XPDAN_SETUP'] = str(2)

# setup glbl
from xpdan.glbl import an_glbl

# import functionality
from xpdan.qt_gui import XpdanSearch, an
from xpdan.data_reduction import *

db = an_glbl.exp_db # alias
