import os

import shed.event_streams as es
from streamz.core import Stream

from xpdan.db_utils import _timestampstr
from ..glbl import an_glbl

import tifffile

source = Stream()

# dark sub images
# sub background images (if possible)
# run human calibration
# run recalibration
# run polarization correction
# run masking
# run recalibration
# write to file
