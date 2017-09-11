"""Save tiff pipeline"""
import os
from operator import sub

import shed.event_streams as es
import tifffile
from shed.event_streams import star

from bluesky.callbacks.broker import LiveImage
from streamz import Stream
from xpdan.db_utils import query_dark, temporal_prox
from xpdan.dev_utils import _timestampstr
from xpdan.formatters import render_and_clean
from xpdan.io import dump_yml
from xpdan.pipelines.pipeline_utils import (if_dark, base_template)


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
    print('start pipeline configuration')
    light_template = os.path.join(save_dir, base_template)
    raw_source = Stream(stream_name='Raw Data')  # raw data
    source = es.fill_events(db, raw_source)  # filled raw data

    # DARK PROCESSING

    # if not dark do dark subtraction
    if_not_dark_stream = es.filter(lambda x: not if_dark(x), source,
                                   input_info={0: ((), 0)},
                                   document_name='start',
                                   stream_name='If not dark',
                                   full_event=True)
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
    if vis:
        dark_sub_fg.sink(star(LiveImage('img')))

    if write_to_disk:
        eventify_raw_descriptor = es.Eventify(
            if_not_dark_stream, stream_name='eventify raw descriptor',
            document='descriptor')
        exts = ['.tiff']
        eventify_input_streams = [dark_sub_fg]
        input_infos = [
            {'data': ('img', 0), 'file': ('filename', 1)},
        ]
        saver_kwargs = [{}]
        eventifies = [es.Eventify(
            s, stream_name='eventify {}'.format(s.stream_name)) for s in
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
        streams_to_be_saved = [dark_sub_fg]
        save_callables = [tifffile.imsave]
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

        [es.map(writer_templater,
                es.zip_latest(es.zip(s1, s2, stream_name='zip render and data',
                                     zip_type='truncate'), made_dir,
                              stream_name='zl dirs and render and data'
                              ),
                input_info=ii,
                output_info=[('final_filename', {'dtype': 'str'})],
                stream_name='Write {}'.format(s1.stream_name),
                **kwargs) for s1, s2, made_dir, ii, writer_templater, kwargs
         in
         zip(
             streams_to_be_saved,
             mega_render,
             make_dirs,  # prevent run condition btwn dirs and files
             input_infos,
             save_callables,
             saver_kwargs
         )]

        es.map(dump_yml, es.zip(eventify_raw_start, md_render),
               input_info={0: (('data', 'filename'), 1),
                           1: (('data',), 0)},
               full_event=True,
               stream_name='dump yaml')
    return raw_source
