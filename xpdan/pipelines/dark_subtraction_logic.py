from operator import sub

from streamz import Stream
from xpdan.db_utils import query_dark, temporal_prox
import shed.event_streams as es


def configure_dark_sub(db):
    fg_dark_stream_source = Stream(
        stream_name='Foreground for background subtraction')
    # Find the dark data and un-pack it
    dark_query = es.Query(db,
                          fg_dark_stream_source,
                          query_function=query_dark,
                          query_decider=temporal_prox,
                          stream_name='Query for FG Dark')
    dark_query_results = es.QueryUnpacker(db, dark_query)
    # # Do the dark subtraction
    dark_sub_fg = es.map(sub,
                         es.zip_latest(fg_dark_stream_source,
                                       dark_query_results),
                         input_info={0: ('pe1_image', 0),
                                     1: ('pe1_image', 1)},
                         output_info=[('img', {'dtype': 'array',
                                               'source': 'testing'})],
                         md=dict(stream_name='Dark Subtracted Foreground',
                                 analysis_stage='dark_sub')
                         )
    return (fg_dark_stream_source, dark_query,
            dark_query_results,
            dark_sub_fg)
