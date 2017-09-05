import os
from pathlib import Path

from xpdan.dev_utils import _timestampstr


def if_dark(doc):
    return doc.get('dark_frame', False)


def if_query_results(n_hdrs):
    return n_hdrs != 0


def if_calibration(start):
    print(list(start.keys()))
    return 'is_calibration' in start
    # return 'detector_calibration_server_uid' in start


def if_not_calibration(doc):
    return 'calibration_md' in doc


def dark_template_func(timestamp, template):
    """Format template for dark images

    Parameters
    ----------
    timestamp: float
        The time in unix epoch
    template: str
        The string to be formatted

    Returns
    -------
    str:

    """
    d = {'human_timestamp': _timestampstr(timestamp), 'ext': '.tiff'}
    t = template.format(**d)
    os.makedirs(os.path.split(t)[0])
    return t


def templater1_func(doc, template):
    """format base string with data from experiment, sample_name,
    folder_tag"""
    d = {'sample_name': doc.get('sample_name', ''),
         'folder_tag': doc.get('folder_tag', '')}
    return template.format(**d)


def templater2_func(doc, template, aux=None, short_aux=None):
    """format with auxiliary and time"""
    if aux is None:
        aux = ['temperature', 'diff_x', 'diff_y', 'eurotherm']
    if short_aux is None:
        short_aux = ['temp', 'x', 'y', 'euro']
    aux_res = ['{}={}'.format(b, doc['data'].get(a, ''))
               for a, b in zip(aux, short_aux)]

    aux_res_str = '_'.join(aux_res)
    # Add a separator between timestamp and extras
    if aux_res_str:
        aux_res_str = '_' + aux_res_str
    return template.format(
        # Change to include name as well
        auxiliary=aux_res_str,
        human_timestamp=_timestampstr(doc['time']))


def templater3_func(template, analysis_stage='raw', ext='.tiff'):
    return Path(template.format(analysis_stage=analysis_stage,
                                ext=ext)).as_posix()
