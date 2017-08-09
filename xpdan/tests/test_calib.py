"""module to test run_calibration"""
import os
import yaml
import uuid
import time
import shutil
import unittest
import numpy as np
from pathlib import Path

from pyFAI.calibration import Calibration
from pyFAI.geometry import Geometry
from xpdan.calib import _configure_calib_instance

from pkg_resources import resource_filename as rs_fn
#rs_dir = rs_fn('xpdan', '/')
#pytest_dir = rs_fn('xpdan', 'tests/')

def test_configure_calib():
    c = _configure_calib_instance(None, None, wavelength=1234)
    # calibrant is None, which default to Ni
    assert c.calibrant.__repr__().split(' ')[0] == 'Ni'  # no magic
    # detector is None, which default to Perkin detector 
    assert c.detector.get_name() == 'Perkin detector'

    c2 = _configure_calib_instance(None, None, wavelength=999)
    # wavelength is given, so it should get customized value
    assert c2.wavelength == 999 * 10 ** (-10)
