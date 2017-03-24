import os
import sys

# expected code
# 0 -> beamline
# 1 -> test
# 2 -> simulation
os.environ['XPDAN_SETUP'] = str(0)

# setup glbl
from xpdan.glbl import an_glbl

db = an_glbl.exp_db # alias
