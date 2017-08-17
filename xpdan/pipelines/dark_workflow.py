"""Example for XPD data"""
import os

import shed.event_streams as es
from streamz.core import Stream

from xpdan.db_utils import _timestampstr
from ..glbl import an_glbl

import tifffile

source = Stream(name='Raw Dark')


def dark_template_func(timestamp, template):
    """Format template for dark images

    Parameters
    ----------
    timestamp: float
        The time in unix epoch
    template: str
        The string to be formatted

    Returns
    -------
    str:

    """
    d = {'human_timestamp': _timestampstr(timestamp), 'ext': 'tiff'}
    t = template.format(**d)
    os.makedirs(os.path.split(t)[0])
    return t

print(an_glbl)

dark_template = os.path.join(
    an_glbl['tiff_base'], 'dark/{human_timestamp}.{ext}')

dark_template_stream = es.map(dark_template_func, source,
                              template=dark_template,
                              full_event=True,
                              input_info={'timestamp': 'time'},
                              output_info=[('file_path', {'dtype': 'str'})])

dark_writer = es.map(tifffile.imsave,
                     es.zip(source, dark_template_stream),
                     input_info={'data': ('pe1_image', 0),
                                 'file': ('file_path', 1)},
                     output_info=[('output_file', {'dtype': None})])
