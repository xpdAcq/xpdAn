"""Decider between pipelines"""

import shed.event_streams as es

from streamz import Stream
from xpdan.pipelines.dark_workflow import source as dark_source
from xpdan.pipelines.pipeline_chunks import (fg_dark_stream_source,
                                             dark_sub_fg,
                                             if_not_dark_stream_source,
                                             foreground_stream,
                                             get_make_calibration)

# from databroker import db
db = None

source = Stream(stream_name='Raw Data')


# Dark logic
def if_dark(docs):
    doc = docs[0]
    tv = 'is_dark' in doc
    return tv


def if_not_dark(docs):
    doc = docs[0]
    tv = 'is_dark' in doc
    return ~tv


# If Dark send to disk
if_dark_stream = es.filter(if_dark, source, document_name='start',
                           input_info=None)
if_dark_stream.connect(dark_source)

# If Not Dark continue
if_not_dark_stream = es.filter(if_not_dark, source, document_name='start',
                               input_info=None)

if_not_dark_stream.connect(fg_dark_stream_source)

# push dark corrected data into background subtraction setup
dark_sub_fg.connect(if_not_dark_stream_source)

# pull data from background subtraction
foreground_stream.connect(get_make_calibration)
# if calibration stream go run calibration and get calibration data back

# if not calibration stream get calibration data

# polarization correction

# mask

# integration

# PDF

# Refinement

source.visualize()
