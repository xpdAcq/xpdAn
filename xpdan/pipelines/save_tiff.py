"""Decider between pipelines"""
import os
import re
from collections import defaultdict
from operator import sub
from pathlib import Path

import shed.event_streams as es
import tifffile
import yaml
from shed.event_streams import star

from bluesky.callbacks.broker import LiveImage
from streamz import Stream
from xpdan.db_utils import query_dark, temporal_prox
from xpdan.dev_utils import _timestampstr
from xpdan.formatters import PartialFormatter, CleanFormatter
from xpdan.pipelines.pipeline_utils import (if_dark)


# TODO: refactor templating
def conf_save_tiff_pipeline(db, save_dir, *, write_to_disk=False, vis=True,
                            image_data_key='pe1_image'):
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
    image_data_key: str, optional
        The key for the image data, defaults to `pe1_image`

    Returns
    -------
    source: Stream
        The source for the graph

    See also
    --------
    xpdan.tools.mask_img
    """
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
    # pdf_stream.sink(pprint)
    if vis:
        dark_sub_fg.sink(star(LiveImage('img')))

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
        [dark_sub_fg, ]]

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
    # [es.map(lambda **x: pprint(x['data']['filename']), cs,
    #         full_event=True) for cs in clean_streams]

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
        iis = [{'data': ('img', 0), 'file': ('filename', 1)}, ]

        writer_streams = [
            es.map(writer_templater,
                   es.zip_latest(s1, s2),
                   input_info=ii,
                   output_info=[('final_filename', {'dtype': 'str'})],
                   **kwargs) for s1, s2, ii, writer_templater, kwargs in
            zip(
                [dark_sub_fg],
                clean_streams,
                iis,
                [tifffile.imsave],
                [{}]
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
