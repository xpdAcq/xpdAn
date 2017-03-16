"""This module is for instantiation of Callbacks"""
from .callbacks_core import XpdAcqLiveTiffExporter
from .glbl import an_glbl
# xpdAcq standard instantiation
import os

template = os.path.join(an_glbl['tiff_base'], '{start.sample_name}')
data_fields = ['temperature', 'diff_x', 'diff_y', 'eurotherm'] # known devices
xpdacq_tiff_export = XpdAcqLiveTiffExporter('pe1_image', template,
                                            data_fields, overwrite=True)
