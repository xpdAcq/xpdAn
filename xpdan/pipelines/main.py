"""Main XPD analysis pipeline"""
import os
from operator import sub, truediv

import numpy as np
import shed.event_streams as es
import tifffile
from shed.event_streams import dstar, star

from bluesky.callbacks.broker import LiveImage
from bluesky.callbacks.core import CallbackBase
from skbeam.core.utils import q_to_twotheta
from skbeam.io.fit2d import fit2d_save
from skbeam.io.save_powder_output import save_output
from streamz import Stream
from xpdan.callbacks import StartStopCallback
from xpdan.db_utils import query_dark, temporal_prox, query_background
from xpdan.dev_utils import _timestampstr
from xpdan.formatters import render_and_clean
from xpdan.io import pdf_saver, dump_yml, poni_saver
from xpdan.pipelines.pipeline_utils import (if_dark, if_query_results,
                                            if_calibration, if_not_calibration,
                                            base_template)
from xpdan.tools import (pull_array, event_count,
                         integrate, generate_binner, load_geo,
                         polarization_correction, mask_img, add_img,
                         pdf_getter, fq_getter, overlay_mask)
from xpdview.callbacks import LiveWaterfall
from ..calib import img_calibration, _save_calib_param

_s = set()


class PrinterCallback(CallbackBase):
    def __init__(self):
        self.analysis_stage = None

    def start(self, doc):
        self.analysis_stage = doc[1]['analysis_stage']

    def event(self, doc):
        print('file saved 1at {}'.format(doc[0]['data']['filename']))
        super().event(doc)


def conf_main_pipeline(db, save_dir, *, write_to_disk=False, vis=True,
                       polarization_factor=.99,
                       image_data_key='pe1_image',
                       mask_setting='default',
                       mask_kwargs=None,
                       pdf_config=None,
                       calibration_md_folder='../xpdConfig/'
                       ):
    """Total data processing pipeline for XPD

    Parameters
    ----------
    db: databroker.broker.Broker instance
        The databroker holding the data, this must be specified as a `db=` in
        the function call (keyword only argument)
    save_dir: str
        The folder in which to save the data, this must be specified as a
        `save_dir=` in the function call (keyword only argument)
    write_to_disk: bool, optional
        If True write files to disk, defaults to False
    vis: bool, optional
        If True visualize the data. Defaults to False
    polarization_factor : float, optional
        polarization correction factor, ranged from -1(vertical) to +1
        (horizontal). default is 0.99. set to None for no
        correction.
    mask_setting : str, optional
        If 'default' reuse mask created for first image, if 'auto' mask all
        images, if None use no mask. Defaults to 'default'
    mask_kwargs : dict, optional
        dictionary stores options for automasking functionality.
        default is defined by an_glbl.auto_mask_dict.
        Please refer to documentation for more details
    image_data_key: str, optional
        The key for the image data, defaults to `pe1_image`
    pdf_config: dict, optional
        Configuration for making PDFs, see pdfgetx3 docs. Defaults to
        ``dict(dataformat='QA', qmaxinst=28, qmax=22)``
    calibration_md_folder: str
        Path to where the calibration is stored for xpdAcq

    Returns
    -------
    source: Stream
        The source for the graph

    See also
    --------
    xpdan.tools.mask_img
    """
    _pdf_config = dict(dataformat='QA', qmaxinst=28, qmax=22)
    if pdf_config is None:
        pdf_config = _pdf_config.copy()
    else:
        pdf_config = _pdf_config.copy().update(**pdf_config)
    if mask_kwargs is None:
        mask_kwargs = {}
    print('start pipeline configuration')
    light_template = os.path.join(save_dir, base_template)
    raw_source = Stream(stream_name='Raw Data')  # raw data
    source = es.fill_events(db, raw_source)  # filled raw data

    if_not_dark_stream = es.filter(lambda x: not if_dark(x), source,
                                   input_info={0: ((), 0)},
                                   document_name='start',
                                   stream_name='If not dark',
                                   full_event=True)
    if_not_dark_stream.sink(star(StartStopCallback()))
    eventify_raw_start = es.Eventify(if_not_dark_stream,
                                     stream_name='eventify raw start')
    h_timestamp_stream = es.map(_timestampstr, if_not_dark_stream,
                                input_info={0: 'time'},
                                output_info=[('human_timestamp',
                                              {'dtype': 'str'})],
                                full_event=True,
                                stream_name='human timestamp')

    # only the primary stream
    if_not_dark_stream_primary = es.filter(lambda x: x[0]['name'] == 'primary',
                                           if_not_dark_stream,
                                           document_name='descriptor',
                                           stream_name='Primary')

    dark_query = es.Query(db,
                          if_not_dark_stream,
                          query_function=query_dark,
                          query_decider=temporal_prox,
                          stream_name='Query for FG Dark')
    dark_query_results = es.QueryUnpacker(db, dark_query,
                                          stream_name='Unpack FG Dark')
    # Do the dark subtraction
    zlid = es.zip_latest(if_not_dark_stream_primary,
                         dark_query_results,
                         stream_name='Combine darks and lights')
    dark_sub_fg = es.map(sub,
                         zlid,
                         input_info={0: (image_data_key, 0),
                                     1: (image_data_key, 1)},
                         output_info=[('img', {'dtype': 'array',
                                               'source': 'testing'})],
                         md=dict(stream_name='Dark Subtracted Foreground',
                                 analysis_stage='dark_sub'))

    # BACKGROUND PROCESSING
    # Query for background
    bg_query_stream = es.Query(db, if_not_dark_stream,
                               query_function=query_background,
                               query_decider=temporal_prox,
                               stream_name='Query for Background')

    # Decide if there is background data
    if_background_stream = es.filter(if_query_results, bg_query_stream,
                                     full_event=True,
                                     input_info={'n_hdrs': (('n_hdrs',), 0)},
                                     document_name='start',
                                     stream_name='If background')
    # if has background do background subtraction
    bg_stream = es.QueryUnpacker(db, if_background_stream,
                                 stream_name='Unpack background')
    bg_dark_stream = es.QueryUnpacker(
        db, es.Query(db,
                     bg_stream,
                     query_function=query_dark,
                     query_decider=temporal_prox,
                     stream_name='Query for BG Dark'),
        stream_name='Unpack background dark')
    # Perform dark subtraction on everything
    dark_sub_bg = es.map(sub,
                         es.zip_latest(bg_stream, bg_dark_stream,
                                       stream_name='Combine bg and bg dark'),
                         input_info={0: (image_data_key, 0),
                                     1: (image_data_key, 1)},
                         output_info=[('img', {'dtype': 'array',
                                               'source': 'testing'})],
                         stream_name='Dark Subtracted Background')

    # bundle the backgrounds into one stream
    bg_bundle = es.BundleSingleStream(dark_sub_bg, bg_query_stream,
                                      name='Background Bundle')

    # sum the backgrounds

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
    fg_bg = es.zip_latest(dark_sub_fg, ave_bg,
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
    eventify_nhdrs = es.Eventify(bg_query_stream, 'n_hdrs',
                                 output_info=[('n_hdrs', {})])
    zldb = es.zip_latest(dark_sub_fg, eventify_nhdrs)
    if_not_background_stream = es.filter(
        lambda x: not if_query_results(x),
        zldb,
        input_info={'x': (('data', 'n_hdrs',), 1)},
        stream_name='If not background',
        full_event=True)
    if_not_background_split_stream = es.split(if_not_background_stream, 2)

    # union of background and not background branch
    foreground_stream = fg_sub_bg.union(
        if_not_background_split_stream.split_streams[0])
    foreground_stream.stream_name = 'Pull from either bgsub or not sub'
    # CALIBRATION PROCESSING

    # if calibration send to calibration runner
    zlfi = es.zip_latest(foreground_stream, es.zip(
        if_not_dark_stream, eventify_raw_start), clear_on_lossless_stop=True)
    if_calibration_stream = es.filter(if_calibration,
                                      zlfi,
                                      input_info={0: ((), 1)},
                                      full_event=True,
                                      document_name='start',
                                      stream_name='If calibration')

    # detector and calibration are under 'detector' and 'dSpacing'
    calibration_stream = es.map(img_calibration, if_calibration_stream,
                                input_info={
                                    'img': (('data', 'img'), 0),
                                    'wavelength': (
                                        ('data', 'bt_wavelength',), 2),
                                    'calibrant': (('data', 'dSpacing',), 2),
                                    'detector': (('data', 'detector',), 2)},
                                output_info=[
                                    ('calibration',
                                     {'dtype': 'object',
                                      'source': 'workflow',
                                      'instance': 'pyFAI.calibration.'
                                                  'Calibration'
                                      }),
                                    ('geo',
                                     {'dtype': 'object',
                                      'source': 'workflow',
                                      'instance': 'pyFAI.azimuthalIntegrator'
                                                  '.AzimuthalIntegrator'})],
                                stream_name='Run Calibration',
                                md={'analysis_stage': 'calib'},
                                full_event=True)
    # write calibration info into xpdAcq sacred place
    es.map(_save_calib_param,
           es.zip(calibration_stream, h_timestamp_stream),
           calib_yml_fp=os.path.join(calibration_md_folder,
                                     'xpdAcq_calib_info.yml'),
           input_info={'calib_c': (('data', 'calibration'), 0),
                       'timestr': (('data', 'human_timestamp'), 1)},
           output_info=[('calib_config_dict', {'dtype': 'dict'})])

    # else get calibration from header
    if_not_calibration_stream = es.filter(if_not_calibration,
                                          if_not_dark_stream,
                                          input_info={0: ((), 0)},
                                          document_name='start',
                                          full_event=True,
                                          stream_name='If not calibration')
    cal_md_stream = es.Eventify(if_not_calibration_stream,
                                'calibration_md',
                                output_info=[('calibration_md',
                                              {'dtype': 'dict',
                                               'source': 'workflow'})],
                                stream_name='Eventify Calibration')
    loaded_cal_stream = es.map(load_geo, cal_md_stream,
                               input_info={'cal_params': 'calibration_md'},
                               output_info=[('geo',
                                             {'dtype': 'object',
                                              'source': 'workflow',
                                              'instance':
                                                  'pyFAI.azimuthalIntegrator'
                                                  '.AzimuthalIntegrator'})],
                               stream_name='Load geometry')

    # union the calibration branches
    loaded_calibration_stream = loaded_cal_stream.union(calibration_stream)
    loaded_calibration_stream.stream_name = 'Pull from either md or ' \
                                            'run calibration'

    # send calibration and corrected images to main workflow
    # polarization correction
    # SPLIT INTO TWO NODES
    zlfl = es.zip_latest(foreground_stream, loaded_calibration_stream,
                         stream_name='Combine FG and Calibration',
                         clear_on_lossless_stop=True)
    p_corrected_stream = es.map(polarization_correction,
                                zlfl,
                                input_info={'img': ('img', 0),
                                            'geo': ('geo', 1)},
                                output_info=[('img', {'dtype': 'array',
                                                      'source': 'testing'})],
                                polarization_factor=polarization_factor,
                                stream_name='Polarization corrected img')
    # generate masks
    if mask_setting is None:
        zlfc = es.zip_latest(es.filter(lambda x: x == 1,
                                       p_corrected_stream,
                                       input_info={0: 'seq_num'},
                                       full_event=True),
                             loaded_calibration_stream,
                             clear_on_lossless_stop=True)
        mask_stream = es.map(lambda x: np.ones(x.shape, dtype=bool),
                             zlfc,
                             input_info={'x': ('img', 0)},
                             output_info=[('mask', {'dtype': 'array',
                                                    'source': 'testing'})],
                             stream_name='dummy mask',
                             md=dict(analysis_stage='mask')
                             )
    else:
        if mask_setting == 'default':
            # note that this could become a much fancier filter
            # eg make a mask every 5th image
            zlfc = es.zip_latest(es.filter(lambda x: x == 1,
                                           p_corrected_stream,
                                           input_info={0: 'seq_num'},
                                           full_event=True),
                                 loaded_calibration_stream,
                                 clear_on_lossless_stop=True)
        else:
            zlfc = es.zip_latest(p_corrected_stream, loaded_calibration_stream,
                                 clear_on_lossless_stop=True)

        zlfc_ds = es.zip_latest(zlfc, if_not_dark_stream,
                                clear_on_lossless_stop=True)
        if_setup_stream = es.filter(
            lambda sn: sn == 'Setup',
            zlfc_ds,
            input_info={0: (('sample_name',), 2)},
            document_name='start',
            full_event=True,
            stream_name='Is Setup Mask'
        )
        blank_mask_stream = es.map(lambda x: np.ones(x.shape, dtype=bool),
                                   if_setup_stream,
                                   input_info={'x': ('img', 0)},
                                   output_info=[('mask',
                                                 {'dtype': 'array',
                                                  'source': 'testing'})],
                                   stream_name='dummy setup mask',
                                   md=dict(analysis_stage='mask')
                                   )
        if_not_setup_steam = es.filter(
            lambda doc: doc.get('sample_name') != 'Setup',
            zlfc_ds,
            input_info={0: ((), 2)},
            document_name='start',
            full_event=True,
            stream_name='Is Not Setup Mask'
        )

        not_setup_mask_stream = es.map(mask_img,
                                       if_not_setup_steam,
                                       input_info={'img': ('img', 0),
                                                   'geo': ('geo', 1)},
                                       output_info=[('mask',
                                                     {'dtype': 'array',
                                                      'source': 'testing'})],
                                       **mask_kwargs,
                                       stream_name='Mask',
                                       md=dict(analysis_stage='mask'))

        mask_stream = not_setup_mask_stream.union(blank_mask_stream)
        mask_stream.stream_name = 'If Setup pull Dummy Mask, else Mask'
    # generate binner stream
    zlmc = es.zip_latest(mask_stream, loaded_calibration_stream,
                         clear_on_lossless_stop=True)

    binner_stream = es.map(generate_binner,
                           zlmc,
                           input_info={'geo': ('geo', 1),
                                       'mask': ('mask', 0)},
                           output_info=[('binner', {'dtype': 'function',
                                                    'source': 'testing'})],
                           img_shape=(2048, 2048),
                           stream_name='Binners')
    zlpb = es.zip_latest(p_corrected_stream, binner_stream,
                         clear_on_lossless_stop=True)

    iq_stream = es.map(integrate,
                       zlpb,
                       input_info={'img': ('img', 0),
                                   'binner': ('binner', 1)},
                       output_info=[('q', {'dtype': 'array',
                                           'source': 'testing'}),
                                    ('iq', {'dtype': 'array',
                                            'source': 'testing'})],
                       stream_name='I(Q)',
                       md=dict(analysis_stage='iq_q'))

    iq_rs_zl = es.zip_latest(iq_stream, eventify_raw_start)

    # convert to tth
    tth_stream = es.map(lambda q, wavelength: np.rad2deg(
        q_to_twotheta(q, wavelength)),
                        iq_rs_zl,
                        input_info={'q': ('q', 0),
                                    'wavelength': ('bt_wavelength', 1)},
                        output_info=[('tth', {'dtype': 'array',
                                              'units': 'degrees'})])

    tth_iq_stream = es.map(lambda **x: (x['tth'], x['iq']),
                           es.zip(tth_stream, iq_stream),
                           input_info={'tth': ('tth', 0),
                                       'iq': ('iq', 1)},
                           output_info=[('tth', {'dtype': 'array',
                                                 'source': 'testing'}),
                                        ('iq', {'dtype': 'array',
                                                'source': 'testing'})],
                           stream_name='Combine tth and iq',
                           md=dict(analysis_stage='iq_tth')
                           )

    fq_stream = es.map(fq_getter,
                       iq_rs_zl,
                       input_info={0: ('q', 0), 1: ('iq', 0),
                                   'composition': ('composition_string', 1)},
                       output_info=[('q', {'dtype': 'array'}),
                                    ('fq', {'dtype': 'array'}),
                                    ('config', {'dtype': 'dict'})],
                       dataformat='QA', qmaxinst=28, qmax=22,
                       md=dict(analysis_stage='fq'))
    pdf_stream = es.map(pdf_getter,
                        iq_rs_zl,
                        input_info={0: ('q', 0), 1: ('iq', 0),
                                    'composition': ('composition_string', 1)},
                        output_info=[('r', {'dtype': 'array'}),
                                     ('pdf', {'dtype': 'array'}),
                                     ('config', {'dtype': 'dict'})],
                        **pdf_config,
                        md=dict(analysis_stage='pdf'))
    if vis:
        foreground_stream.sink(star(LiveImage(
            'img', window_title='Dark Subtracted Image', cmap='viridis')))
        zlpm = es.zip_latest(p_corrected_stream, mask_stream,
                             clear_on_lossless_stop=True)
        masked_img = es.map(overlay_mask,
                            zlpm,
                            input_info={'img': (('data', 'img'), 0),
                                        'mask': (('data', 'mask'), 1)},
                            full_event=True,
                            output_info=[('overlay_mask', {'dtype': 'array'})])
        masked_img.sink(star(LiveImage('overlay_mask',
                                       window_title='Dark/Background/'
                                                    'Polarization Corrected '
                                                    'Image with Mask',
                                       cmap='viridis',
                                       limit_func=lambda im: (
                                           np.nanpercentile(im, 1),
                                           np.nanpercentile(im, 99))
                                       # norm=LogNorm()
                                       )))
        iq_stream.sink(star(LiveWaterfall('q', 'iq',
                                          units=('Q (A^-1)', 'Arb'),
                                          window_title='I(Q)')))
        tth_iq_stream.sink(star(LiveWaterfall('tth', 'iq',
                                              units=('tth', 'Arb'),
                                              window_title='I(tth)')))
        fq_stream.sink(star(LiveWaterfall('q', 'fq',
                                          units=('Q (A^-1)', 'F(Q)'),
                                          window_title='F(Q)')))
        pdf_stream.sink(star(LiveWaterfall('r', 'pdf',
                                           units=('r (A)', 'G(r) A^-2'),
                                           window_title='G(r)')))

    if write_to_disk:
        eventify_raw_descriptor = es.Eventify(
            if_not_dark_stream, stream_name='eventify raw descriptor',
            document='descriptor')
        exts = ['.tiff', '', '_Q.chi',
                '_tth.chi', '.gr',
                '.poni']
        eventify_input_streams = [dark_sub_fg, mask_stream, iq_stream,
                                  tth_iq_stream, pdf_stream,
                                  calibration_stream]
        input_infos = [
            {'data': ('img', 1), 'file': ('filename', 0)},
            {'mask': ('mask', 1), 'filename': ('filename', 0)},
            {'tth': ('q', 1), 'intensity': ('iq', 1),
             'output_name': ('filename', 0)},
            {'tth': ('tth', 1), 'intensity': ('iq', 1),
             'output_name': ('filename', 0)},
            {'r': ('r', 1), 'pdf': ('pdf', 1), 'filename': ('filename', 0),
             'config': ('config', 1)},
            {'calibration': ('calibration', 1), 'filename': ('filename', 0)}
        ]
        saver_kwargs = [{}, {}, {'q_or_2theta': 'Q', 'ext': ''},
                        {'q_or_2theta': '2theta', 'ext': ''}, {}, {}]
        eventifies = [
            es.Eventify(s,
                        stream_name='eventify {}'.format(s.stream_name)) for s
            in
            eventify_input_streams]

        mega_render = [
            es.map(render_and_clean,
                   es.zip_latest(
                       es.zip(h_timestamp_stream,
                              # human readable event timestamp
                              if_not_dark_stream,  # raw events,
                              stream_name='mega_render zip'
                              ),
                       eventify_raw_start,
                       eventify_raw_descriptor,
                       analysed_eventify
                   ),
                   string=light_template,
                   input_info={
                       'human_timestamp': (('data', 'human_timestamp'), 0),
                       'raw_event': ((), 1),
                       'raw_start': (('data',), 2),
                       'raw_descriptor': (('data',), 3),
                       'analyzed_start': (('data',), 4)
                   },
                   ext=ext,
                   full_event=True,
                   output_info=[('filename', {'dtype': 'str'})],
                   stream_name='mega render '
                               '{}'.format(analysed_eventify.stream_name)
                   )
            for ext, analysed_eventify in zip(exts, eventifies)]

        streams_to_be_saved = [dark_sub_fg, mask_stream, iq_stream,
                               tth_iq_stream, pdf_stream, calibration_stream]

        save_callables = [tifffile.imsave, fit2d_save, save_output,
                          save_output, pdf_saver, poni_saver]

        md_render = es.map(render_and_clean,
                           eventify_raw_start,
                           string=light_template,
                           input_info={'raw_start': (('data',), 0), },
                           output_info=[('filename', {'dtype': 'str'})],
                           ext='.yml',
                           full_event=True,
                           stream_name='MD render')

        make_dirs = [es.map(lambda x: os.makedirs(os.path.split(x)[0],
                                                  exist_ok=True),
                            cs,
                            input_info={0: 'filename'},
                            output_info=[('filename', {'dtype': 'str'})],
                            stream_name='Make dirs {}'.format(cs.stream_name)
                            ) for cs in mega_render]

        _s.update([es.map(writer_templater,
                          es.zip_latest(
                              es.zip(s2, s1, stream_name='zip render and data',
                                     zip_type='truncate'), made_dir,
                              stream_name='zl dirs and render and data'
                              ),
                          input_info=ii,
                          output_info=[('final_filename', {'dtype': 'str'})],
                          stream_name='Write {}'.format(s1.stream_name),
                          **kwargs) for
                   s1, s2, made_dir, ii, writer_templater, kwargs
                   in
                   zip(
                       streams_to_be_saved,
                       mega_render,
                       make_dirs,  # prevent run condition btwn dirs and files
                       input_infos,
                       save_callables,
                       saver_kwargs
                   )])

        _s.add(es.map(dump_yml, es.zip(eventify_raw_start, md_render),
                      input_info={0: (('data', 'filename'), 1),
                                  1: (('data',), 0)},
                      full_event=True,
                      stream_name='dump yaml'))
        [es.zip(cs,
                streams_to_be_s, zip_type='truncate',
                stream_name='zip_print'
                ).sink(star(PrinterCallback())
                       ) for cs, streams_to_be_s in zip(
            mega_render, streams_to_be_saved)]
    print('Finish pipeline configuration')
    return raw_source
