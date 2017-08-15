from itertools import islice
from pprint import pprint
from .dev_utils import _timestampstr
import collections


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
        The dictionary containing {'key-value':[list of scan indices]}.
        For example a search over 'sample_name' might return
        {'Ni':[0,1,2,3,4,9,10],'gold nanoparticles':[5,6,7,8]}
    """
    d = {}
    for i, hdr in enumerate(hdrs):
        if key in hdr['start'].keys():
            if hdr['start'][key] in d.keys():
                d[hdr['start'][key]].append(i)
            else:
                d[hdr['start'][key]] = [i]
    if verbose:
        pprint(d)
    return d


def scan_diff(hdrs, verbose=True, blacklist=None):
    """Get the metadata differences between scans in hdrs list

    Parameters
    ----------
    hdrs: list of Header objects
        The headers to be diffed
    verbose: bool, optional
        If true prints the results. Defaults to True
    blacklist: list of str, optional
        List of keys to not be included in diff. If None, defaults to `uid`

    Returns
    -------
    dict:
        The dictionary of keys with at least one different value across the
        scans. The values are the results for each header.
    """
    if blacklist is None:
        blacklist = ['uid']
    keys = set([k for hdr in hdrs for k in hdr['start'].keys()
                if k not in blacklist])
    kv = {}
    for k in keys:
        # TODO: eventually support dict differences
        # See http://stackoverflow.com/a/11092607/5100330
        v = [hdr['start'][k] for hdr in hdrs if k in hdr['start'].keys() if
             isinstance(hdr['start'][k], collections.Hashable)]
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
        'Specify a list of fields to summarize. If None, the following
        will be returned
        ``['sample_name', 'sp_type', 'sp_startingT', 'sp_endingT']``
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
        data = [hdr['start'][key] for key in fields if key in hdr['start'].keys()]
        data2 = {key: hdr['start'][key] for key in fields if
                 key in hdr['start'].keys()}

        data2['time'] = _timestampstr(hdr['start']['time'])
        data = [_timestampstr(hdr['start']['time'])] + data

        data2['uid'] = hdr['start']['uid'][:6]
        data += [hdr['start']['uid'][:6]]

        data = [str(d) for d in data]
        data = '_'.join(data)
        if verbose:
            print((i, data2))
        datas.append(data)
    return datas


def query_dark(db, docs, schema=1):
    """Get dark data from databroker

    Parameters
    ----------
    db: Broker instance
    docs: tuple of dict

    Returns
    -------

    """
    if schema == 1:
        doc = docs[0]
        return db(uid=doc['sc_dk_field_uid'])


def query_background(db, docs, schema=1):
    if schema == 1:
        doc = docs[0]
        return db(sample_name=doc['bkgd_sample_name'],
                  is_dark={'$exists': False})


def temporal_prox(res, docs):
    doc = docs[0]
    t = doc['time']
    dt_sq = [(t - r['start']['time']) ** 2 for r in res]
    i = dt_sq.index(min(dt_sq))
    min_r = next(islice(res, i, i + 1))
    return min_r