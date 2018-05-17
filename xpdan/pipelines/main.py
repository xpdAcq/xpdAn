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
from xpdtools.pipelines.raw_pipeline import (mask_setting, geometry_img_shape,
                                             iq_comp, composition, wavelength,
                                             calibrant, detector,
                                             is_calibration_img, geo_input,
                                             raw_background_dark,
                                             raw_foreground_dark, img_counter,
                                             raw_foreground, gen_geo_cal,
                                             dark_corrected_foreground, q,
                                             mean, tth, mask, pdf, fq, sq,
                                             bg_corrected_img,
                                             pol_corrected_img, raw_background)
from xpdtools.tools import overlay_mask
from xpdview.callbacks import LiveWaterfall

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
start_yaml_string = (start_docs.map(lambda s: {'raw_start': s,
                                               'ext': '.yaml',
                                               # TODO: talk to BLS ask if
                                               #  they like this
                                               # 'analysis_stage': 'meta'
                                               })
                     .map(lambda kwargs, string: render(string, **kwargs),
                          string=base_template)
                     )
start_yaml_string.map(clean_template).zip(start_docs).starsink(dump_yml)

# create filename string
filename_node = all_docs.map(lambda kwargs, string: render(string, **kwargs),
                             string=base_template,
                             stream_name='base path')

# SAVING NAMES
filename_name_nodes = {}
for name, analysis_stage, ext in zip(
        ['dark_corrected_image_name', 'iq_name', 'tth_name', 'mask_fit2d_name',
         'mask_np_name', 'pdf_name', 'fq_name', 'sq_name'],
        ['dark_sub', 'iq', 'itth', 'mask', 'mask', 'pdf', 'fq', 'sq'],
        ['.tiff', '', '_tth', '', '_mask.npy', '.gr', '.fq', '.sq']
):
    if ext:
        temp_name_node = filename_node.map(render,
                                           analysis_stage=analysis_stage,
                                           ext=ext)
    else:
        temp_name_node = filename_node.map(render,
                                           analysis_stage=analysis_stage)

    filename_name_nodes[name] = temp_name_node.map(clean_template)
    (filename_name_nodes[name].map(os.path.dirname)
     .sink(os.makedirs, exist_ok=True,))


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
for ds in [raw_foreground_dark, raw_background, raw_background_dark]:
    FromEventStream('start', source).map(ds.emit(0.0))

bg_query = (start_docs.map(query_background, db=db))
bg_docs = (bg_query
           .zip(start_docs)
           .starmap(temporal_prox)
           .filter(lambda x: x is not None)
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
                 fg_dark_query
                 .map(lambda x: x[0].documents(fill=True)).flatten()
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

# '''
# SAVING
# May rethink how we are doing the saving. If the saving was attached to the
# translation nodes then it would be run before the rest of the graph was
# processed.

# This could be done by having each saver inside a callback which takes both
# analyzed and raw documents, and creates the path from those two.

# dark corrected img
(filename_name_nodes['dark_corrected_image_name']
 .zip(dark_corrected_foreground)
 .starsink(imsave, stream_name='dark corrected foreground'))
# integrated intensities
(q.combine_latest(mean, emit_on=1).zip(filename_name_nodes['iq_name'])
 .map(lambda l: (*l[0], l[1]))
 .starsink(save_output, 'Q',
           stream_name='save integration {}'.format('Q')))
(tth.combine_latest(mean, emit_on=1).zip(filename_name_nodes['tth_name'])
 .map(lambda l: (*l[0], l[1]))
 .starsink(save_output, '2theta',
           stream_name='save integration {}'.format('tth')))
# Mask
(mask.zip_latest(filename_name_nodes['mask_fit2d_name'])
 .sink(lambda x: fit2d_save(np.flipud(x[0]), x[1])))
(mask.zip_latest(filename_name_nodes['mask_np_name'])
 .sink(lambda x: np.save(x[1], x[0])))
# PDF
(pdf.zip(filename_name_nodes['pdf_name']).map(lambda l: (*l[0], l[1]))
 .starsink(pdf_saver, stream_name='pdf saver'))
# F(Q)
(fq.zip(filename_name_nodes['fq_name']).map(lambda l: (*l[0], l[1]))
 .starsink(pdf_saver, stream_name='fq saver'))
# S(Q)
(sq.zip(filename_name_nodes['sq_name']).map(lambda l: (*l[0], l[1]))
 .starsink(pdf_saver, stream_name='sq saver'))
# '''
# Visualization
# background corrected img
em_background_corrected_img = ToEventStream(bg_corrected_img,
                                            ('image',)).starsink(
    LiveImage('image', window_title='Background_corrected_img',
              cmap='viridis'))

# polarization corrected img with mask overlayed
ToEventStream(
    pol_corrected_img.combine_latest(mask).starmap(overlay_mask),
    ('image',)).starsink(LiveImage('image', window_title='final img',
                                   limit_func=lambda im: (
                                       np.nanpercentile(im, 2.5),
                                       np.nanpercentile(im, 97.5)
                                   ), cmap='viridis'))

# integrated intensities
iq_em = (ToEventStream(mean
         .combine_latest(q, emit_on=0), ('iq', 'q'))
         .starsink(LiveWaterfall('q', 'iq', units=('1/A', 'Intensity'),
                                 window_title='{} vs {}'.format('iq', 'q')),
                   stream_name='{} {} vis'.format('q', 'iq')))

(ToEventStream(mean
               .combine_latest(tth, emit_on=0), ('iq', 'tth'))
 .starsink(LiveWaterfall('tth', 'iq', units=('Degree', 'Intensity'),
                         window_title='{} vs {}'.format('iq', 'tth')),
           stream_name='{} {} vis'.format('tth', 'iq')))
# F(Q)
ToEventStream(fq, ('q', 'fq')).starsink(
    LiveWaterfall('q', 'fq', units=('1/A', 'Intensity'),
                  window_title='F(Q)'), stream_name='F(Q) vis')

# PDF
ToEventStream(pdf, ('r', 'gr')).starsink(
    LiveWaterfall('r', 'gr', units=('A', '1/A**2'),
                  window_title='PDF'), stream_name='G(r) vis')

raw_source.starsink(StartStopCallback())
# raw_source.visualize(os.path.expanduser('~/mystream.png'), source_node=True)
