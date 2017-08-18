from operator import sub, add, truediv

import shed.event_streams as es

from streamz import Stream
from xpdan.db_utils import query_dark, temporal_prox, query_background
from xpdan.pipelines.dark_subtraction_chunk import dark_sub_fg
from xpdan.tools import pull_array, event_count

db = None

# Figure out if there is a background to subtract
if_not_dark_stream_source = Stream(name='dark_corrected_data')
bg_query_stream = es.Query(db, if_not_dark_stream_source,
                           query_function=query_background,
                           query_decider=temporal_prox,
                           name='Query for Background')


# If there is background data
def if_background(docs):
    doc = docs[0]
    return doc['n_hdrs'] > 0


# If there is no background data
def if_not_background(docs):
    doc = docs[0]
    return doc['n_hdrs'] == 0


if_background_stream = es.filter(if_background, bg_query_stream,
                                 full_event=True, input_info=None,
                                 document_name='start')
if_not_background_stream = es.filter(if_not_background, bg_query_stream,
                                     full_event=True, input_info=None,
                                     document_name='start')

bg_stream = es.QueryUnpacker(db, if_background_stream)
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

foreground_stream = fg_sub_bg.union(if_not_background_stream)

get_or_make_calibration = Stream()


def if_calibration(docs):
    doc = docs[0]
    return 'calibration_server_uid' in doc


def if_not_calibration(docs):
    doc = docs[0]
    return not 'calibration_server_uid' in doc

# if calibration stream go run calibration and get calibration data back
if_calibration_stream = es.filter(if_calibration, get_or_make_calibration,
                                  full_event=True, input_info=None,
                                  document_name='start')

# if not calibration stream get calibration data
if_not_calibration_stream = es.filter(if_calibration, get_or_make_calibration,
                                      full_event=True, input_info=None,
                                      document_name='start')

cal_md_stream = es.Eventify(source, start_key='calibration_md',
                            output_info=[('calibration_md',
                                          {'dtype': 'dict',
                                           'source': 'workflow'})],
                            md=dict(name='Calibration'))
cal_stream = es.map(load_geo, cal_md_stream,
                    input_info={'cal_params': 'calibration_md'},
                    output_info=[('geo',
                                  {'dtype': 'object', 'source': 'workflow'})])
