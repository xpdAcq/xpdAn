from pyxdameraulevenshtein import \
    normalized_damerau_levenshtein_distance as ndld
import numpy as np
from databroker.broker import _munge_time


def getFromDict(dataDict, mapList):
    """ Get a value from a nested dictionary, given a list of keys

    Parameters
    ----------
    dataDict: dict
        The dictionary to be queried
    mapList: list of str
        A list of strings, each string is one level lower than the previous

    Returns
    -------
    object:
        The the value from the dict

    """
    for k in mapList:
        dataDict = dataDict[k]
    return dataDict


def nested_dict_values(d):
    """Yield all string values inside a nested dictionary

    Parameters
    ----------
    d: dict
        The dictionary to be unpacked

    Yields
    -------
    str:
        The string value inside the dictionary

    """
    for v in d.values():
        if isinstance(v, dict):
            yield from nested_dict_values(v)
        else:
            if isinstance(v, str):
                yield v
            else:
                yield None


def fuzzy_search(db, keys, search_string, size=100):
    """Fuzzy search a databroker for given keys

    Parameters
    ----------
    db: databroker.DataBroker instance
        The databroker to be searched
    keys: list of str
        The list of strings to be accessed
    search_string: str
        The string to be searched for
    size: int or 'all', optional
        The number of results to be returned, if 'all' all are returned.
         Defaults to 100 results

    Returns
    -------
    list:
        A list

    """
    if isinstance(keys, list):
        scores = []
        for h in db():
            scores.append(ndld(getFromDict(h['start'], keys), search_string))
        scores = [ndld(getFromDict(h['start'], keys), search_string) for h in
                  db()]
    else:
        scores = [ndld(h['start'][keys], search_string) for h in db()]
    zipped = zip(scores, db())
    zipped = sorted(zipped, key=lambda x: x[0])
    hdrs = [x for (y, x) in zipped]
    return hdrs[:size]


def super_fuzzy_search(db, search_string, size=100):
    scores = []
    for h in db():
        internal_scores = [ndld(v, search_string) for v in
                           nested_dict_values(h['start']) if v is not None]
        scores.append(min(internal_scores))
    zipped = zip(scores, db())
    zipped = sorted(zipped, key=lambda x: x[0])
    hdrs = [x for (y, x) in zipped]
    if size == 'all':
        return hdrs
    elif isinstance(size, int):
        return hdrs[:size]


def beamtime_dates(db, keys=['facility', 'beamline',
                             'saf']):
    hdrs = db()
    safs = set([h['start']['bt_safN'] for h in hdrs])
    returns = []
    for s in safs:
        hdrs = db(bt_safN=s)
        st = [h['start']['time'] for h in hdrs]
        start_hdr = hdrs[np.argmin[st]]
        stop_hdr = hdrs[np.argmax[st]]
        info = {k: start_hdr[k] for k in keys}
        info.update({'start_time': _munge_time(start_hdr['start']['time']),
                     'stop_time': _munge_time(stop_hdr['start']['time'])})
        returns.append(info)
    return returns
