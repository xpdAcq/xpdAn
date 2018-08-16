import os

import numpy as np
from shed.translation import FromEventStream
from streamz_ext import Stream
from tifffile import imsave
from xpdan.callbacks import StartStopCallback
from xpdan.db_utils import query_background, query_dark, temporal_prox
from xpdan.formatters import render, clean_template
from xpdan.io import dump_yml
from xpdan.pipelines.pipeline_utils import (_timestampstr,
                                            Filler,
                                            base_template)
from xpdconf.conf import glbl_dict
from xpdtools.pipelines.raw_pipeline import (
    raw_foreground_dark,
    raw_foreground,
    dark_corrected_foreground)

image_name = glbl_dict['image_field']
db = glbl_dict['exp_db']
calibration_md_folder = {'folder': 'xpdAcq_calib_info.yml'}

filler = Filler(db=db)
# Build the general pipeline from the raw_pipeline
raw_source = Stream(stream_name='raw source')

# TODO: change this when new dark logic comes
# Check that the data isn't a dark
dk_uid = (FromEventStream('start', (), upstream=raw_source)
          .map(lambda x: 'sc_dk_field_uid' in x))
# Fill the raw event stream
source = (raw_source
          .combine_latest(dk_uid)
          .filter(lambda x: x[1])
          .pluck(0)
          # Filler returns None for resource/datum data
          .starmap(filler).filter(lambda x: x is not None))
# Get all the documents
start_docs = FromEventStream('start', (), source)
descriptor_docs = FromEventStream('descriptor', (), source,
                                  event_stream_name='primary')
event_docs = FromEventStream('event', (), source, event_stream_name='primary')
all_docs = (event_docs
            .combine_latest(start_docs, descriptor_docs, emit_on=0)
            .starmap(lambda e, s, d: {'raw_event': e, 'raw_start': s,
                                      'raw_descriptor': d,
                                      'human_timestamp': _timestampstr(
                                          s['time'])}))

# If new calibration uid invalidate our current calibration cache
start_timestamp = FromEventStream('start', ('time',), source)

# Clean out the cached darks and backgrounds on start
# so that this will run regardless of background/dark status
# note that we get the proper data (if it exists downstream)
start_docs.sink(lambda x: raw_foreground_dark.emit(0.0))

# Get foreground dark
fg_dark_query = (start_docs.map(query_dark, db=db))
fg_dark_query.filter(lambda x: x != [] and isinstance(x, list)).sink(print)
fg_dark_query.filter(lambda x: x == []).sink(lambda x: print('No dark found!'))
(FromEventStream('event', ('data', image_name),
                 fg_dark_query.filter(lambda x: x != [])
                 .map(lambda x: x if not isinstance(x, list) else x[0])
                 .map(lambda x: x.documents(fill=True)).flatten()
                 ).map(np.float32)
 .connect(raw_foreground_dark))
(FromEventStream('event', ('data', image_name),
                 source, event_stream_name='dark'
                 ).map(np.float32)
 .connect(raw_foreground_dark))

# Get foreground
(FromEventStream('event', ('data', image_name), source, principle=True,
                 event_stream_name='primary',
                 stream_name='raw_foreground').map(np.float32)
 .connect(raw_foreground))

# Save out calibration data to special place
h_timestamp = start_timestamp.map(_timestampstr)

raw_source.starsink(StartStopCallback())
# '''
# SAVING
# May rethink how we are doing the saving. If the saving was attached to the
# translation nodes then it would be run before the rest of the graph was
# processed.

# This could be done by having each saver inside a callback which takes both
# analyzed and raw documents, and creates the path from those two.

start_yaml_string = (start_docs.map(lambda s: {'raw_start': s,
                                               'ext': '.yaml',
                                               'analysis_stage': 'meta'
                                               })
                     .map(lambda kwargs, string, **kwargs2: render(string,
                                                                   **kwargs,
                                                                   **kwargs2),
                          string=base_template,
                          base_folder=glbl_dict['tiff_base'])
                     )
start_yaml_string.map(clean_template).zip(start_docs).starsink(dump_yml)

# create filename string
filename_node = all_docs.map(
    lambda kwargs, string, **kwargs2: render(string, **kwargs, **kwargs2),
    string=base_template,
    stream_name='base path',
    base_folder=glbl_dict['tiff_base'])

# SAVING NAMES
filename_name_nodes = {}
for name, analysis_stage, ext in zip(['dark_corrected_image_name'],
                                     ['dark_sub'], ['.tiff']):
    if ext:
        temp_name_node = filename_node.map(render,
                                           analysis_stage=analysis_stage,
                                           ext=ext)
    else:
        temp_name_node = filename_node.map(render,
                                           analysis_stage=analysis_stage)

    filename_name_nodes[name] = temp_name_node.map(clean_template)
    (filename_name_nodes[name].map(os.path.dirname)
     .sink(os.makedirs, exist_ok=True, ))

# dark corrected img
(filename_name_nodes['dark_corrected_image_name']
 .zip(dark_corrected_foreground)
 .starsink(imsave, stream_name='dark corrected foreground'))

save_kwargs = start_yaml_string.kwargs
filename_node.kwargs = save_kwargs
