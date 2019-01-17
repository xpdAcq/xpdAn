from collections import defaultdict

import copy
import os
import shutil
import subprocess
from collections import defaultdict
import time

from bluesky.callbacks.core import CallbackBase
from databroker._core import _sanitize
from databroker.assets.path_only_handlers import \
    AreaDetectorTiffPathOnlyHandler
from databroker.utils import ensure_path_exists


class StartStopCallback(CallbackBase):

    def __init__(self):
        self.t0 = 0

    def start(self, doc):
        self.t0 = time.time()
        print('START ANALYSIS ON {}'.format(doc['uid']))

    def stop(self, doc):
        print('FINISH ANALYSIS ON {}'.format(doc.get('run_start', 'NA')))
        print('Analysis time {}'.format(time.time() - self.t0))
