from pprint import pprint
from .dev_utils import _timestampstr


def sort_scans_by_hdr_key(hdrs, key, verbose=True):
    """In a list of hdrs, group the scans by header-key.

        Use this function to find all the scans in the list of headers that 
        have a particular key value, such as 'sample_name'='Ni'

        Function returns a list of indices for the position in the list, 
        associated with the metadata key values.


    Parameters
    ----------
    hdrs: list of Header objects
        The dictionary containing {'key-value':[list of scan indices]}. 
        For example a search over 'sample_name' might return 
        {'Ni':[0,1,2,3,4,9,10],'gold nanoparticles':[5,6,7,8]}

    key: str
        The scans will be sorted by the values of this metadata key, 
        e.g., 'sample_name'
    verbose: bool, optional
        If true prints the results. Defaults to True

    Returns
    -------
    dict:
        The dictionary of unique values and positions
    """
    d = {}
    for i, hdr in enumerate(hdrs):
        if key in hdr.start.keys():
            if hdr.start[key] in d.keys():
                d[hdr.start[key]].append(i)
            else:
                d[hdr.start[key]] = [i]
    if verbose:
        pprint(d)
    return d


def scan_diff(hdrs, verbose=True):
    """Get the metadata differences between scans

    Parameters
    ----------
    hdrs: list of Header objects
        The headers to be diffed
    verbose: bool, optional
        If true prints the results. Defaults to True

    Returns
    -------
    dict:
        The dictionary of keys with different values across the scans.
        The values are the results for each header.
    """
    keys = set([k for hdr in hdrs for k in hdr.start.keys()])
    kv = {}
    for k in keys:
        v = [hdr.start[k] for hdr in hdrs if k in hdr.start.keys()]
        if len(set(v)) != 1:
           kv[k] = v
    if verbose:
        pprint(kv)
    return kv


def scan_summary(hdrs, fields=None, verbose=True):
    """Provide one line summaries of headers

    Parameters
    ----------
    hdrs: list of headers
        The headers from the databroker
    fields: list of str, optional
        The fields to be included in the summary, if None use
        `['sample_name', 'temperature', 'diff_x', 'diff_y', 'eurotherm']`
        defaults to None
    verbose: bool, optional
        If True print the summary

    Returns
    -------
    list:
        List of summary strings
    """
    if fields is None:
        fields = ['sample_name', 'sp_type', 'sp_startingT', 'sp_endingT']
    fields = fields
    datas = []
    for i, hdr in enumerate(hdrs):
        data = [hdr.start[key] for key in fields if key in hdr.start.keys()]
        data2 = {key: hdr.start[key] for key in fields if key in hdr.start.keys()}

        data2['time'] = _timestampstr(hdr.start['time'])
        data = [_timestampstr(hdr.start['time'])] + data

        data2['uid'] = hdr.start['uid'][:6]
        data += [hdr.start['uid'][:6]]

        data = [str(d) for d in data]
        data = '_'.join(data)
        if verbose:
            print((i, data2))
        datas.append(data)
    return datas
