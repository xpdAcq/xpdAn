"""Example for XPD data"""
import os

import shed.event_streams as es
from streams.core import Stream

from xpdan.db_utils import _timestampstr
from ..glbl import an_glbl

import tifffile

source = Stream(name='Raw Dark')


def write_dark(doc, template):
    d = {'human_timestamp': _timestampstr(doc.ge('timestamp', None)),
         'ext': 'tiff'}
    full_file_path = template.format(**d)
    tifffile.imsave(full_file_path)


dark_template = os.path.join(
    an_glbl['tiff_base'], 'dark/{human_timestamp}.{ext}')

dark_writer = es.map(write_dark, source, dark_template, full_event=True)

