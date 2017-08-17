"""Example for XPD data"""
from operator import add, sub, truediv

import shed.event_streams as es
from streams.core import Stream

from databroker import db
from xpdan.db_utils import query_dark, query_background, temporal_prox
from xpdan.tools import (better_mask_img, iq_to_pdf, pull_array,
                         generate_binner, z_score_image, integrate,
                         polarization_correction, load_geo, event_count)

source = Stream(name='Raw')

# foreground logic
fg_dark_stream = es.QueryUnpacker(db, es.Query(db, source,
                                               query_function=query_dark,
                                               query_decider=temporal_prox,
                                               name='Query for FG Dark'))

dark_sub_fg = es.map(sub,
                     es.zip(source,
                            fg_dark_stream),
                     input_info={'img1': ('pe1_image', 0),
                                 'img2': ('pe1_image', 1)},
                     output_info=[('img', {'dtype': 'array',
                                           'source': 'testing'})],
                     name='Dark Subtracted Foreground',
                     analysis_stage='dark_sub'
                     )

bg_query_stream = es.Query(db, source,
                           query_function=query_background,
                           query_decider=temporal_prox,
                           name='Query for Background')

bg_stream = es.QueryUnpacker(db, bg_query_stream)
bg_dark_stream = es.QueryUnpacker(db, es.Query(db, bg_stream,
                                               query_function=query_dark,
                                               query_decider=temporal_prox,
                                               name='Query for BG Dark'))

# Perform dark subtraction on everything
dark_sub_bg = es.map(sub,
                     es.zip(bg_stream, bg_dark_stream),
                     input_info={'img1': ('pe1_image', 0),
                                 'img2': ('pe1_image', 1)},
                     output_info=[('img', {'dtype': 'array',
                                           'source': 'testing'})])

# bundle the backgrounds into one stream
bg_bundle = es.BundleSingleStream(dark_sub_bg, bg_query_stream,
                                  name='Background Bundle')

# sum the backgrounds
summed_bg = es.accumulate(add, bg_bundle, start=pull_array,
                          state_key='img1',
                          input_info={'img2': 'img'},
                          output_info=[('img', {
                              'dtype': 'array',
                              'source': 'testing'})])

count_bg = es.accumulate(event_count, bg_bundle, start=1,
                         state_key='count',
                         output_info=[('count', {
                             'dtype': 'int',
                             'source': 'testing'})])

ave_bg = es.map(truediv, es.zip(summed_bg, count_bg),
                input_info={'0': ('img', 0), '1': ('count', 1)},
                output_info=[('img', {
                    'dtype': 'array',
                    'source': 'testing'})],
                # name='Average Background'
                )

# combine the fg with the summed_bg
fg_bg = es.combine_latest(dark_sub_fg, ave_bg, emit_on=dark_sub_fg)

# subtract the background images
fg_sub_bg = es.map(sub,
                   fg_bg,
                   input_info={'img1': ('img', 0),
                               'img2': ('img', 1)},
                   output_info=[('img', {'dtype': 'array',
                                         'source': 'testing'})],
                   # name='Background Corrected Foreground'
                   )

# make/get calibration stream
cal_md_stream = es.Eventify(source, start_key='calibration_md',
                            output_info=[('calibration_md',
                                          {'dtype': 'dict',
                                           'source': 'workflow'})],
                            md=dict(name='Calibration'))
cal_stream = es.map(load_geo, cal_md_stream,
                    input_info={'cal_params': 'calibration_md'},
                    output_info=[('geo',
                                  {'dtype': 'object', 'source': 'workflow'})])

# polarization correction
# SPLIT INTO TWO NODES
pfactor = .99
p_corrected_stream = es.map(polarization_correction,
                            es.zip_latest(fg_sub_bg, cal_stream),
                            input_info={'img': ('img', 0),
                                        'geo': ('geo', 1)},
                            output_info=[('img', {'dtype': 'array',
                                                  'source': 'testing'})],
                            polarization_factor=pfactor)

# generate masks
mask_kwargs = {'bs_width': None}
mask_stream = es.map(better_mask_img,
                     es.zip_latest(p_corrected_stream,
                                   cal_stream),
                     input_info={'img': ('img', 0),
                                 'geo': ('geo', 1)},
                     output_info=[('mask', {'dtype': 'array',
                                            'source': 'testing'})],
                     **mask_kwargs)

# generate binner stream
binner_stream = es.map(generate_binner,
                       cal_stream,
                       input_info={'geo': 'geo'},
                       output_info=[('binner', {'dtype': 'function',
                                                'source': 'testing'})],
                       img_shape=(2048, 2048))

iq_stream = es.map(integrate,
                   es.zip_latest(p_corrected_stream,
                                 binner_stream),
                   input_info={'img': ('img', 0),
                               'binner': ('binner', 1)},
                   output_info=[('iq', {'dtype': 'array',
                                        'source': 'testing'})])

# z-score the data
z_score_stream = es.map(z_score_image,
                        es.zip_latest(p_corrected_stream,
                                      binner_stream),
                        input_info={'img': ('img', 0),
                                    'binner': ('binner', 1)},
                        output_info=[('z_score_img', {'dtype': 'array',
                                                      'source': 'testing'})])

pdf_stream = es.map(iq_to_pdf, es.zip(iq_stream, source))