from pprint import pprint


def need_name_here(hdrs, key, verbose=True):
    """Build a dictionary where the keys are the unique values for the given
    key and the values are the positions of the headers for that key

    Parameters
    ----------
    hdrs: list of Header objects
        The headers to be searched
    key: str
        The key to be searched for
    verbose: bool, optional
        If true prints the results. Defaults to True

    Returns
    -------
    dict:
        The dictionary of unique values and positions
    """
    d = {}
    for i, hdr in enumerate(hdrs):
        if hdr.start[key] in d.keys():
            d[key].append(i)
        else:
            d[key] = [i]
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
        v = [hdr[k] for hdr in hdrs]
        if len(set(v)) != 1:
           kv[k] = v
    if verbose:
        pprint(kv)
    return kv


def scan_headlines(hdrs, fields=None, verbose=True):
    if fields is None:
        fields = ['temperature', 'diff_x', 'diff_y', 'eurotherm']
    fields = ['timestamp'] + fields
    datas = []
    for i, hdr in enumerate(hdrs):
        data = [hdr.start.get(key, '') for key in fields]
        data = '_'.join(data)
        if verbose:
            pprint((i, data))
        datas.append(data)
    return datas
