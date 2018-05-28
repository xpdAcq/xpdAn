import os

import numpy as np
from bluesky.callbacks.broker import LiveImage
from shed.translation import FromEventStream, ToEventStream
from skbeam.io import save_output
from skbeam.io.fit2d import fit2d_save
from streamz_ext import Stream
from tifffile import imsave
from xpdan.db_utils import query_background, query_dark, temporal_prox
from xpdan.formatters import render, clean_template
from xpdan.io import pdf_saver, dump_yml
from xpdan.pipelines.pipeline_utils import (_timestampstr,
                                            clear_combine_latest, Filler,
                                            base_template)
from xpdan.callbacks import StartStopCallback
from xpdconf.conf import glbl_dict
from xpdtools.calib import _save_calib_param
# from xpdtools.pipelines.raw_pipeline import (geometry_img_shape,
#                                              iq_comp, composition, wavelength,
#                                              calibrant, detector,
#                                              is_calibration_img, geo_input,
#                                              raw_background_dark,
#                                              raw_foreground_dark, img_counter,
#                                              raw_foreground, gen_geo_cal,
#                                              dark_corrected_foreground, q,
#                                              mean, tth, mask, pdf, fq, sq,
#                                              pol_corrected_img, raw_background)
from xpdtools.pipelines.raw_pipeline import *
from xpdtools.pipelines.raw_pipeline import (mask_setting,  # noqa: F401
                                             )
from xpdtools.tools import overlay_mask

image_name = glbl_dict['image_field']
db = glbl_dict['exp_db']
mask_setting.update(setting='first')
calibration_md_folder = {'folder': 'xpdAcq_calib_info.yml'}

filler = Filler(db=db)
# Build the general pipeline from the raw_pipeline
raw_source = Stream(stream_name='raw source')

# TODO: change this when new dark logic comes
# Check that the data isn't a dark
dk_uid = (
    FromEventStream('start', (), upstream=raw_source)
    .map(lambda x: 'sc_dk_field_uid' in x)
)
# Fill the raw event stream
source = (
    raw_source
    .combine_latest(dk_uid)
    .filter(lambda x: x[1])
    .pluck(0)
    .starmap(filler)
)
# Get all the documents
start_docs = FromEventStream('start', (), source)
descriptor_docs = FromEventStream('descriptor', (), source,
                                  event_stream_name='primary')
event_docs = FromEventStream('event', (), source, event_stream_name='primary')
all_docs = (
    event_docs
    .combine_latest(start_docs, descriptor_docs, emit_on=0)
    .starmap(lambda e, s, d: {'raw_event': e, 'raw_start': s,
                              'raw_descriptor': d,
                              'human_timestamp': _timestampstr(s['time'])})
)

# If new calibration uid invalidate our current calibration cache
(FromEventStream('start', ('detector_calibration_client_uid',), source)
 .unique(history=1)
 .map(lambda x: geometry_img_shape.lossless_buffer.clear()))

# Clear composition every start document
(FromEventStream('start', (), source)
 .sink(lambda x: clear_combine_latest(iq_comp, 1)))
FromEventStream('start', ('composition_string',), source).connect(composition)

# Calibration information
(FromEventStream('start', ('bt_wavelength',), source)
 .unique(history=1)
 .connect(wavelength))
(FromEventStream('start', ('calibrant',), source)
 .unique(history=1)
 .connect(calibrant))
(FromEventStream('start', ('detector',), source)
 .unique(history=1)
 .connect(detector))

(FromEventStream('start', (), source).
 map(lambda x: 'detector_calibration_server_uid' in x).
 connect(is_calibration_img))
# Only pass through new calibrations (prevents us from recalculating cals)
(FromEventStream('start', ('calibration_md',), source).
 unique(history=1).
 connect(geo_input))

start_timestamp = FromEventStream('start', ('time',), source)

# Clean out the cached darks and backgrounds on start
# so that this will run regardless of background/dark status
# note that we get the proper data (if it exists downstream)
start_docs.sink(lambda x: raw_background_dark.emit(0.0))
start_docs.sink(lambda x: raw_background.emit(0.0))
start_docs.sink(lambda x: raw_foreground_dark.emit(0.0))

bg_query = (start_docs.map(query_background, db=db))
bg_docs = (bg_query
           .zip(start_docs)
           .starmap(temporal_prox)
           .filter(lambda x: x != [])
           .map(lambda x: x[0].documents(fill=True))
           .flatten())

# Get bg dark
bg_dark_query = (FromEventStream('start', (), bg_docs)
                 .map(query_dark, db=db)
                 )
(FromEventStream('event', ('data', image_name),
                 bg_dark_query.map(lambda x: x[0].documents(fill=True))
                 .flatten())
 .connect(raw_background_dark))

# Get background
FromEventStream('event', ('data', image_name), bg_docs).connect(raw_background)

# Get foreground dark
fg_dark_query = (start_docs
                 .map(query_dark, db=db))
fg_dark_query.filter(lambda x: x == []).sink(lambda x: print('No dark found!'))
(FromEventStream('event', ('data', image_name),
                 fg_dark_query.filter(lambda x: x != [])
                 .map(lambda x: x.documents(fill=True)).flatten()
                 )
 .connect(raw_foreground_dark))

# Get foreground
FromEventStream('event', ('seq_num',), source, stream_name='seq_num'
                ).connect(img_counter)
FromEventStream('event', ('data', image_name), source, principle=True,
                stream_name='raw_foreground').connect(raw_foreground)

# Save out calibration data to special place
h_timestamp = start_timestamp.map(_timestampstr)
(gen_geo_cal
 .zip_latest(h_timestamp)
 .sink(lambda x: _save_calib_param(*x, calibration_md_folder['file_path'])))

raw_source.starsink(StartStopCallback())
# raw_source.visualize(os.path.expanduser('~/mystream.png'), source_node=True)
