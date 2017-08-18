"""Decider between pipelines"""

import shed.event_streams as es
from shed.event_streams import dstar, star
from operator import sub, add, truediv
from streamz import Stream
# from xpdan.pipelines.dark_workflow import source as dark_source
from xpdan.db_utils import query_dark, temporal_prox, query_background
from xpdan.tools import (pull_array, event_count,
                         integrate, generate_binner, load_geo,
                         polarization_correction, z_score_image, mask_img)
from bluesky.callbacks.broker import LiveImage
from xpdview.callbacks import LiveWaterfall
from diffpy.pdfgetx import PDFGetter

from pprint import pprint


def conf_master_pipeline(db):
    # Dark logic
    def if_dark(docs):
        doc = docs[0]
        tv = 'is_dark' in doc
        return tv

    def if_not_dark(docs):
        doc = docs[0]
        tv = 'is_dark' in doc
        return ~tv

    # If there is background data
    def if_background(docs):
        doc = docs[0]
        return doc['n_hdrs'] > 0

    # If there is no background data
    def if_not_background(docs):
        doc = docs[0]
        return doc['n_hdrs'] == 0

    def if_calibration(docs):
        doc = docs[0]
        return 'calibration_server_uid' in doc

    def if_not_calibration(docs):
        doc = docs[0]
        return 'calibration_md' in doc
        # return 'calibration_client_uid' in doc

    source = Stream(stream_name='Raw Data')

    # DARK PROCESSING
    # if dark send to dark writer
    if_dark_stream = es.filter(if_dark, source, input_info=None,
                               document_name='start',
                               stream_name='If dark')
    # if_dark_stream.connect(dark_source)

    # if not dark do dark subtraction
    if_not_dark_stream = es.filter(if_not_dark, source, input_info=None,
                                   document_name='start',
                                   stream_name='If not dark')
    dark_query = es.Query(db,
                          if_not_dark_stream,
                          query_function=query_dark,
                          query_decider=temporal_prox,
                          stream_name='Query for FG Dark')
    dark_query_results = es.QueryUnpacker(db, dark_query,
                                          stream_name='Unpack FG Dark')
    # Do the dark subtraction
    dark_sub_fg = es.map(sub,
                         es.zip_latest(if_not_dark_stream,
                                       dark_query_results,
                                       stream_name='Combine darks and lights'),
                         input_info={0: ('pe1_image', 0),
                                     1: ('pe1_image', 1)},
                         output_info=[('img', {'dtype': 'array',
                                               'source': 'testing'})],
                         md=dict(stream_name='Dark Subtracted Foreground',
                                 analysis_stage='dark_sub')
                         )

    # BACKGROUND PROCESSING
    # Query for background
    # """
    bg_query_stream = es.Query(db, if_not_dark_stream,
                               query_function=query_background,
                               query_decider=temporal_prox,
                               stream_name='Query for Background')

    # Decide if there is background data
    if_background_stream = es.filter(if_background, bg_query_stream,
                                     full_event=True, input_info=None,
                                     document_name='start',
                                     stream_name='If background')
    # if has background do background subtraction
    bg_stream = es.QueryUnpacker(db, if_background_stream,
                                 stream_name='Unpack background')
    bg_dark_stream = es.QueryUnpacker(db, es.Query(db, bg_stream,
                                                   query_function=query_dark,
                                                   query_decider=temporal_prox,
                                                   stream_name='Query for BG Dark'),
                                      stream_name='Unpack background dark')
    # Perform dark subtraction on everything
    dark_sub_bg = es.map(sub,
                         es.zip_latest(bg_stream, bg_dark_stream,
                                       stream_name='Combine bg and bg dark'),
                         input_info={0: ('pe1_image', 0),
                                     1: ('pe1_image', 1)},
                         output_info=[('img', {'dtype': 'array',
                                               'source': 'testing'})],
                         stream_name='Dark Subtracted Background')

    # bundle the backgrounds into one stream
    bg_bundle = es.BundleSingleStream(dark_sub_bg, bg_query_stream,
                                      name='Background Bundle')

    # sum the backgrounds
    def add_img(img1, img2):
        return img1 + img2

    summed_bg = es.accumulate(dstar(add_img), bg_bundle,
                              start=dstar(pull_array),
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
                    input_info={0: ('img', 0), 1: ('count', 1)},
                    output_info=[('img', {
                        'dtype': 'array',
                        'source': 'testing'})],
                    stream_name='Average Background'
                    )

    # combine the fg with the summed_bg
    fg_bg = es.zip_latest(dark_sub_fg, ave_bg, emit_on=dark_sub_fg,
                          stream_name='Combine fg with bg')

    # subtract the background images
    fg_sub_bg = es.map(sub,
                       fg_bg,
                       input_info={0: ('img', 0),
                                   1: ('img', 1)},
                       output_info=[('img', {'dtype': 'array',
                                             'source': 'testing'})],
                       stream_name='Background Corrected Foreground'
                       )

    # else do nothing
    if_not_background_stream = es.filter(if_not_background, bg_query_stream,
                                         input_info=None,
                                         document_name='start',
                                         stream_name='If not background')

    # union of background and not background branch
    foreground_stream = fg_sub_bg.union(if_not_background_stream)
    foreground_stream.stream_name = 'Pull from either bgsub or not sub'

    # CALIBRATION PROCESSING

    # if calibration send to calibration maker
    if_calibration_stream = es.filter(if_calibration, if_not_dark_stream,
                                      input_info=None,
                                      document_name='start',
                                      stream_name='If calibration')

    def run_calibration():
        pass

    run_calibration_stream = es.map(run_calibration, if_calibration_stream,
                                    stream_name='Run Calibration')

    # else get calibration from header
    if_not_calibration_stream = es.filter(if_not_calibration,
                                          if_not_dark_stream,
                                          input_info=None,
                                          document_name='start',
                                          stream_name='If not calibration')
    cal_md_stream = es.Eventify(if_not_calibration_stream,
                                start_key='calibration_md',
                                output_info=[('calibration_md',
                                              {'dtype': 'dict',
                                               'source': 'workflow'})],
                                stream_name='Eventify Calibration')
    cal_stream = es.map(load_geo, cal_md_stream,
                        input_info={'cal_params': 'calibration_md'},
                        output_info=[('geo',
                                      {'dtype': 'object',
                                       'source': 'workflow',
                                       'instance': 'pyFAI.azimuthalIntegrator'
                                                   '.AzimuthalIntegrator'})],
                        stream_name='Load geometry ')

    # union the calibration branches
    loaded_calibration_stream = cal_stream.union(run_calibration_stream)
    loaded_calibration_stream.stream_name = 'Pull from either md or ' \
                                            'run calibration'

    # send calibration and corrected images to main workflow
    # polarization correction
    # SPLIT INTO TWO NODES
    # foreground_stream.sink(star(LiveImage('img')))
    pfactor = .99
    p_corrected_stream = es.map(polarization_correction,
                                es.zip_latest(foreground_stream,
                                              loaded_calibration_stream),
                                input_info={'img': ('img', 0),
                                            'geo': ('geo', 1)},
                                output_info=[('img', {'dtype': 'array',
                                                      'source': 'testing'})],
                                polarization_factor=pfactor,
                                stream_name='Polarization corrected img')

    # generate masks
    # """
    mask_kwargs = {'bs_width': None}
    mask_stream = es.map(mask_img,
                         es.zip_latest(es.filter(lambda x: x == 1,
                                                 p_corrected_stream,
                                                 input_info={0: 'seq_num'},
                                                 full_event=True),
                                       cal_stream),
                         input_info={'img': ('img', 0),
                                     'geo': ('geo', 1)},
                         output_info=[('mask', {'dtype': 'array',
                                                'source': 'testing'})],
                         **mask_kwargs,
                         stream_name='Mask')
    # mask_stream.sink(pprint)
    # mask_stream.sink(star(LiveImage('mask')))

    # generate binner stream
    binner_stream = es.map(generate_binner,
                           es.zip_latest(mask_stream, cal_stream),
                           input_info={'geo': ('geo', 1),
                                       'mask': ('mask', 0)},
                           output_info=[('binner', {'dtype': 'function',
                                                    'source': 'testing'})],
                           img_shape=(2048, 2048),
                           stream_name='Binners')
    """
    binner_stream = es.map(generate_binner,
                           cal_stream,
                           input_info={'geo': ('geo', 0)},
                           output_info=[('binner', {'dtype': 'function',
                                                    'source': 'testing'})],
                           img_shape=(2048, 2048),
                           stream_name='Binners')
    binner_stream.sink(pprint)
    """

    iq_stream = es.map(integrate,
                       es.zip_latest(p_corrected_stream, binner_stream),
                       input_info={'img': ('img', 0),
                                   'binner': ('binner', 1)},
                       output_info=[('q', {'dtype': 'array',
                                           'source': 'testing'}),
                                    ('iq', {'dtype': 'array',
                                            'source': 'testing'})],
                       stream_name='I(Q)')
    # iq_stream.sink(pprint)
    iq_stream.sink(star(LiveWaterfall('q', 'iq')))

    def pdf_getter(*args, **kwargs):
        pg = PDFGetter()
        return pg(*args, **kwargs)

    composition_stream = es.Eventify(if_not_dark_stream,
                                     'sample_name',
                                     output_info=[('composition',
                                                   {'dtype': 'str'})],
                                     stream_name='Sample Composition')
    composition_stream.sink(pprint)
    pdf_stream = es.map(pdf_getter,
                        es.zip_latest(iq_stream, composition_stream),
                        input_info={0: ('q', 0), 1: ('iq', 0),
                                    'composition': ('composition', 1)},
                        output_info=[('r', {'dtype': 'array'}),
                                     ('pdf', {'dtype': 'array'})],
                        dataformat='QA', qmaxinst=28, qmax=22)
    pdf_stream.sink(star(LiveWaterfall('r', 'pdf')))
    """
    # z-score the data
    z_score_stream = es.map(z_score_image,
                            es.zip_latest(p_corrected_stream,
                                          binner_stream),
                            input_info={'img': ('img', 0),
                                        'binner': ('binner', 1)},
                            output_info=[('z_score_img',
                                          {'dtype': 'array',
                                           'source': 'testing'})],
                            stream_name='Z-score-image')
    def iq_to_pdf():
        pass

    def refine_structure():
        pass

    trial_structure_stream = es.Eventify(if_not_dark_stream, 'structure',
                                         output_info=[('stru',
                                                       {'dtype': 'object'})],
                                         stream_name='Trial Structure')

    structure = es.map(refine_structure, es.zip_latest(pdf_stream,
                                                       trial_structure_stream))

    # source.visualize()
    # """
    return source
