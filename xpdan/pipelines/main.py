"""Decider between pipelines"""
import os
import re
from collections import defaultdict
from operator import sub, truediv
from pathlib import Path

import shed.event_streams as es
import tifffile
import yaml
from shed.event_streams import dstar, star

from bluesky.callbacks.broker import LiveImage
from skbeam.io.fit2d import fit2d_save
from skbeam.io.save_powder_output import save_output
from streamz import Stream
from xpdan.db_utils import query_dark, temporal_prox, query_background
from xpdan.dev_utils import _timestampstr
from xpdan.formatters import PartialFormatter, CleanFormatter
from xpdan.io import pdf_saver
from xpdan.pipelines.pipeline_utils import (if_dark, if_query_results,
                                            if_calibration, if_not_calibration)
from xpdan.tools import (pull_array, event_count,
                         integrate, generate_binner, load_geo,
                         polarization_correction, mask_img, add_img,
                         pdf_getter, fq_getter)
from xpdview.callbacks import LiveWaterfall
from pprint import pprint


def conf_main_pipeline(db, save_dir, *, write_to_disk=False, vis=True,
                       polarization_factor=.99,
                       image_data_key='pe1_image',
                       mask_setting='default',
                       mask_kwargs=None,
                       pdf_config=None):
    """Total data processing pipeline for XPD

    Parameters
    ----------
    db: databroker.broker.Broker instance
        The databroker holding the data, this must be specified as a `db=` in
        the function call (keyword only argument)
    write_to_disk: bool, optional
        If True write files to disk, defaults to False
    save_dir: str
        The folder in which to save the data, this must be specified as a
        `save_dir=` in the function call (keyword only argument)
    vis: bool, optional
        If True visualize the data. Defaults to False
    polarization_factor : float, optional
        polarization correction factor, ranged from -1(vertical) to +1
        (horizontal). default is 0.99. set to None for no
        correction.
    mask_setting : str, optional
        If 'default' reuse mask created for first image, if auto mask all
        images. Defaults to 'default'
    mask_kwargs : dict, optional
        dictionary stores options for automasking functionality.
        default is defined by an_glbl.auto_mask_dict.
        Please refer to documentation for more details
    image_data_key: str, optional
        The key for the image data, defaults to `pe1_image`
    pdf_config: dict, optional
        Configuration for making PDFs, see pdfgetx3 docs. Defaults to
        ``dict(dataformat='QA', qmaxinst=28, qmax=22)``

    Returns
    -------
    source: Stream
        The source for the graph

    See also
    --------
    xpdan.tools.mask_img
    """
    if pdf_config is None:
        pdf_config = dict(dataformat='QA', qmaxinst=28, qmax=22)
    if mask_kwargs is None:
        mask_kwargs = {}
    print('start pipeline configuration')
    light_template = os.path.join(
        save_dir,
        '{sample_name}/{folder_tag}/{analysis_stage}/'
        '{sample_name}_{human_timestamp}'
        '_[temp={temperature:1.2f}]'
        '_[dx={diff_x:1.3f}]'
        '_[dy={diff_y:1.3f}]'
        '_{uid:.6}'
        '_{seq_num:03d}{ext}')
    fmt = PartialFormatter()
    source = Stream(stream_name='Raw Data')
    # source.sink(pprint)

    # DARK PROCESSING
    # if dark send to dark writer
    # if_dark_stream = es.filter(if_dark, source, input_info=None,
    #                            document_name='start',
    #                            stream_name='If dark')

    # if not dark do dark subtraction
    if_not_dark_stream = es.filter(lambda x: not if_dark(x), source,
                                   input_info=None,
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
                         input_info={0: (image_data_key, 0),
                                     1: (image_data_key, 1)},
                         output_info=[('img', {'dtype': 'array',
                                               'source': 'testing'})],
                         md=dict(stream_name='Dark Subtracted Foreground',
                                 analysis_stage='dark_sub'))

    # write to disk
    # BACKGROUND PROCESSING
    # Query for background
    # """
    bg_query_stream = es.Query(db, if_not_dark_stream,
                               query_function=query_background,
                               query_decider=temporal_prox,
                               stream_name='Query for Background')

    # Decide if there is background data
    if_background_stream = es.filter(if_query_results, bg_query_stream,
                                     full_event=True, input_info=None,
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
    if_not_background_stream = es.filter(
        lambda x: not if_query_results(x, doc_to_inspect=1),
        es.zip_latest(dark_sub_fg,
                      bg_query_stream),
        input_info=None,
        document_name='start',
        stream_name='If not background')
    if_not_background_split_stream = es.split(if_not_background_stream, 2)

    # union of background and not background branch
    foreground_stream = fg_sub_bg.union(
        if_not_background_split_stream.split_streams[0])
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
    # if_not_calibration_stream.sink(pprint)
    cal_md_stream = es.Eventify(if_not_calibration_stream,
                                'calibration_md',
                                output_info=[('calibration_md',
                                              {'dtype': 'dict',
                                               'source': 'workflow'})],
                                stream_name='Eventify Calibration')
    # cal_md_stream.sink(pprint)
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
    # loaded_calibration_stream.sink(pprint)
    # foreground_stream.sink(pprint)

    # send calibration and corrected images to main workflow
    # polarization correction
    # SPLIT INTO TWO NODES
    zlfl = es.zip_latest(foreground_stream, loaded_calibration_stream,
                         stream_name='Combine FG and Calibration')
    # zlfl.sink(pprint)
    p_corrected_stream = es.map(polarization_correction,
                                zlfl,
                                input_info={'img': ('img', 0),
                                            'geo': ('geo', 1)},
                                output_info=[('img', {'dtype': 'array',
                                                      'source': 'testing'})],
                                polarization_factor=polarization_factor,
                                stream_name='Polarization corrected img')
    # p_corrected_stream.sink(pprint)
    # generate masks
    if mask_setting == 'default':
        zlfc = es.zip_latest(es.filter(lambda x: x == 1,
                                       p_corrected_stream,
                                       input_info={0: 'seq_num'},
                                       full_event=True),
                             cal_stream)
    elif mask_setting == 'auto':
        zlfc = es.zip_latest(p_corrected_stream, cal_stream)
    # zlfc.sink(pprint)
    mask_stream = es.map(mask_img,
                         zlfc,
                         input_info={'img': ('img', 0),
                                     'geo': ('geo', 1)},
                         output_info=[('mask', {'dtype': 'array',
                                                'source': 'testing'})],
                         **mask_kwargs,
                         stream_name='Mask',
                         md=dict(analysis_stage='mask'))
    # mask_stream.sink(pprint)
    # generate binner stream
    zlmc = es.zip_latest(mask_stream, cal_stream)
    # zlmc.sink(pprint)
    binner_stream = es.map(generate_binner,
                           zlmc,
                           input_info={'geo': ('geo', 1),
                                       'mask': ('mask', 0)},
                           output_info=[('binner', {'dtype': 'function',
                                                    'source': 'testing'})],
                           img_shape=(2048, 2048),
                           stream_name='Binners')
    # binner_stream.sink(pprint)
    zlpb = es.zip_latest(p_corrected_stream, binner_stream)
    # zlpb.sink(pprint)
    iq_stream = es.map(integrate,
                       zlpb,
                       input_info={'img': ('img', 0),
                                   'binner': ('binner', 1)},
                       output_info=[('q', {'dtype': 'array',
                                           'source': 'testing'}),
                                    ('iq', {'dtype': 'array',
                                            'source': 'testing'})],
                       stream_name='I(Q)',
                       md=dict(analysis_stage='iq'))

    # iq_stream.sink(pprint)

    composition_stream = es.Eventify(if_not_dark_stream,
                                     # Change this to sample_composition
                                     'sample_name',
                                     output_info=[('composition',
                                                   {'dtype': 'str'})],
                                     stream_name='Sample Composition')
    # composition_stream.sink(pprint)

    fq_stream = es.map(fq_getter,
                       es.zip_latest(iq_stream, composition_stream),
                       input_info={0: ('q', 0), 1: ('iq', 0),
                                   'composition': ('composition', 1)},
                       output_info=[('q', {'dtype': 'array'}),
                                    ('fq', {'dtype': 'array'}),
                                    ('config', {'dtype': 'dict'})],
                       dataformat='QA', qmaxinst=28, qmax=22,
                       md=dict(analysis_stage='fq'))
    # pdf_stream.sink(pprint)
    pdf_stream = es.map(pdf_getter,
                        es.zip_latest(iq_stream, composition_stream),
                        input_info={0: ('q', 0), 1: ('iq', 0),
                                    'composition': ('composition', 1)},
                        output_info=[('r', {'dtype': 'array'}),
                                     ('pdf', {'dtype': 'array'}),
                                     ('config', {'dtype': 'dict'})],
                        **pdf_config,
                        md=dict(analysis_stage='pdf'))
    # pdf_stream.sink(pprint)
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

    # """
    if vis:
        foreground_stream.sink(star(LiveImage('img')))
        mask_stream.sink(star(LiveImage('mask')))
        iq_stream.sink(star(LiveWaterfall('q', 'iq', units=['Q (A^-1)',
                                                            'Arb'])))
        fq_stream.sink(star(LiveWaterfall('q', 'fq', units=['Q (A^-1)',
                                                            'F(Q)'])))
        pdf_stream.sink(star(LiveWaterfall('r', 'pdf', units=['r (A)',
                                                              'G(r) A^-3'])))

    # dark_template = os.path.join(tiff_base,
    #                              'dark/{human_timestamp}_{uid}{ext}')
    # dark_template_stream = es.map(dark_template_func, if_dark_stream,
    #                               template=dark_template,
    #                               full_event=True,
    #                               input_info={'timestamp': 'time'},
    #                               output_info=[
    #                                   ('file_path', {'dtype': 'str'})])

    eventify_raw = es.Eventify(if_not_dark_stream, stream_name='eventify raw')

    h_timestamp_stream = es.map(_timestampstr, if_not_dark_stream,
                                input_info={0: 'time'},
                                output_info=[('human_timestamp',
                                              {'dtype': 'str'})],
                                full_event=True,
                                stream_name='human timestamp')

    render_0 = es.map(lambda a, **x: fmt.format(a, **x),
                      es.zip_latest(es.zip(h_timestamp_stream,
                                           if_not_dark_stream),
                                    eventify_raw),
                      a=light_template,
                      output_info=[('template', {'dtype': 'str'})],
                      stream_name='render 0')

    render_1 = es.map(lambda a, x: fmt.format(a, **x),
                      es.zip(if_not_dark_stream, render_0),
                      input_info={'x': ((), 0),
                                  0: (('data', 'template'), 1)},
                      full_event=True,
                      output_info=[('template', {'dtype': 'str'})],
                      stream_name='render 1')

    eventifies = [
        es.Eventify(s,
                    stream_name='eventify {}'.format(s.stream_name)) for s in
        [dark_sub_fg,
         mask_stream,
         iq_stream,
         pdf_stream]]

    def render_2_func(a, x, ext):
        return fmt.format(a, ext=ext, **x)

    render_2 = [es.map(render_2_func,
                       es.zip_latest(render_1, e),
                       input_info={0: ('template', 0),
                                   1: (('data',), 1)},
                       output_info=[('template',
                                     {'dtype': 'str'})],
                       ext=ext,
                       stream_name='render 2 {}'.format(e.stream_name)
                       ) for e, ext in zip(eventifies,
                                           ['.tiff',
                                            '.msk',
                                            '_Q.chi', '.gr'])]

    # render_2[-1].sink(pprint)
    def clean_template(template, removals=None):
        cfmt = CleanFormatter()
        if removals is None:
            removals = ['temp', 'dx', 'dy']
        d = cfmt.format(template, defaultdict(str))

        for r in removals:
            d = d.replace('[{}=]'.format(r), '')
        z = re.sub(r"__+", "_", d)
        z = z.replace('_.', '.')
        e = z.replace('[', '')
        e = e.replace(']', '')
        e = e.replace('(', '')
        e = e.replace(')', '')
        f = Path(e).as_posix()
        return f

    clean_streams = [es.map(clean_template, s,
                            input_info={0: 'template'},
                            output_info=[('filename', {'dtype': 'str'})],
                            stream_name='clean template '
                                        '{}'.format(s.stream_name)
                            ) for s in render_2]
    make_dirs = [es.map(lambda x: os.makedirs(os.path.split(x)[0],
                                              exist_ok=True), cs,
                        input_info={0: 'filename'},
                        stream_name='Make dirs {}'.format(cs.stream_name)
                        ) for cs in clean_streams]
    # clean_streams[-1].sink(pprint)
    [es.map(lambda **x: pprint(x['data']['filename']), cs,
            full_event=True) for cs in clean_streams]

    render_md_0 = es.map(lambda a, **x: fmt.format(a, **x),
                         eventify_raw,
                         a=light_template,
                         output_info=[('template', {'dtype': 'str'})],
                         ext='md.yml')
    md_cleanup = es.map(clean_template, render_md_0,
                        input_info={0: 'template'},
                        output_info=[('filename', {'dtype': 'str'})])
    # md_cleanup.sink(pprint)

    # """
    if write_to_disk:
        iis = [
            {'data': ('img', 0), 'file': ('filename', 1)},
            {'mask': ('mask', 0), 'filename': ('filename', 1)},
            {'tth': ('q', 0), 'intensity': ('iq', 0),
             'output_name': ('filename', 1)},
            {'r': ('r', 0), 'pdf': ('pdf', 0), 'filename': ('filename', 1),
             'config': ('config', 0)},
        ]

        writer_streams = [
            es.map(writer_templater,
                   es.zip_latest(s1, s2),
                   input_info=ii,
                   output_info=[('final_filename', {'dtype': 'str'})],
                   **kwargs) for s1, s2, ii, writer_templater, kwargs in
            zip(
                [dark_sub_fg, mask_stream, iq_stream, pdf_stream],
                clean_streams,
                iis,
                [tifffile.imsave, fit2d_save, save_output, pdf_saver],
                [{}, {}, {'q_or_2theta': 'Q', 'ext': ''}, {}]
            )]

        def dump_yml(filename, data):
            if not os.path.exists(filename):
                os.makedirs(os.path.split(filename)[0])
            with open(filename, 'w') as f:
                yaml.dump(data, f)

        md_writer = es.map(dump_yml, es.zip(eventify_raw, md_cleanup),
                           input_info={0: (('data', 'filename'), 1),
                                       1: (('data',), 0)},
                           full_event=True)

    # """
    return source
