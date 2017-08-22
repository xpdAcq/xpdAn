"""Enhanced databroker with fuzzy search options"""
##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################

from heapq import heapify, heappushpop
from pprint import pprint
import datetime
import pytz
from pyxdameraulevenshtein import \
    normalized_damerau_levenshtein_distance as ndld

from databroker.broker import Broker


def _munge_time(t, timezone):
    """Close your eyes and trust @arkilic

    Parameters
    ----------
    t : float
        POSIX (seconds since 1970)
    timezone : pytz object
        e.g. ``pytz.timezone('US/Eastern')``

    Return
    ------
    time
        as ISO-8601 format
    """
    t = datetime.date.fromtimestamp(t)
    return timezone.localize(t).replace(microsecond=0).isoformat()


class FuzzyBroker(Broker):
    def fuzzy_search(self, keys, search_string, size=100):
        """Fuzzy search a databroker for given keys

        Parameters
        ----------
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

        Notes
        ------
        This search can take a long time as they turn over the entire
        databroker searching for the ``search_string`` please make a point to
        filter the databroker to shorten the total search time

        """
        heap = [(-1, -1, -1)] * size  # ndld can't return less than 0
        heapify(heap)
        if isinstance(keys, list):
            for h in self():
                # prioritize recent documents
                heappushpop(heap, (1. - ndld(_get_from_dict(h['start'], keys),
                                             search_string),
                                   h['start']['time'] * -1, h))
        else:
            for h in self():
                heappushpop(heap, (1. - ndld(h['start'][keys], search_string),
                                   h['start']['time'] * -1, h))
        heap.sort()
        heap.reverse()
        return [g[-1] for g in heap if g[0] >= 0.]

    def super_fuzzy_search(self, search_string, size=100):
        """Fuzzy search a databroker

        Parameters
        ----------
        search_string: str
            The string to be searched for
        size: int, optional
            The number of results to be returned.
             Defaults to 100 results

        Returns
        -------
        list:
            A list of headers which contain close matches

        Notes
        ------
        This search can take a long time as they turn over the entire
        databroker (and all of the dictionaries inside) searching for the
        ``search_string`` please make a point to filter the databroker to
        shorten the total search time.

        """
        heap = [(-1, -1, -1)] * size  # ndld can't return less than 0
        heapify(heap)
        for h in self():
            internal_scores = [1. - ndld(v, search_string) for v in
                               _nested_dict_values(h['start'])
                               if v is not None]
            heappushpop(heap,
                        (max(internal_scores), h['start']['time'] * -1, h))
        heap.sort()
        heap.reverse()
        return [g[-1] for g in heap if g[0] != -1]

    def beamtime_dates(self, keys=('beamtime_uid', 'bt_safN',
                                   'facility', 'beamline'),
                       beamtime_key='beamtime_uid',
                       print_results=True):
        """Get info for each beamtime

        Parameters
        ----------
        keys: iterable of str
            The keys to be included in the return
        beamtime_key: str
            The key for the unique beamtime key
        print_results: bool
            If true prints the information

        Returns
        -------
        list of dicts:
            The list of beamtimes and their associated information
        """
        hdrs = self()
        bts = set([h['start'][beamtime_key] for h in hdrs])
        returns = []
        for s in bts:
            hdrs = self(**{beamtime_key: s})
            start_hdr = next(iter(hdrs))
            for hdr in hdrs:
                pass
            stop_hdr = hdr
            info = {k: start_hdr[k] for k in keys if k in start_hdr.keys()}
            info.update({'start_time': _munge_time(start_hdr['start']['time'],
                                                   pytz.timezone(
                                                       'US/Eastern')),
                         'stop_time': _munge_time(stop_hdr['start']['time'],
                                                  pytz.timezone(
                                                      'US/Eastern'))})
            returns.append(info)
        if print_results:
            pprint(returns)
        return returns

    def fuzzy_set_search(self, key, search_string, size=100):
        """Return the most similar set of values to the search string.

        Parameters
        ----------
        key: list of str
            The list of strings to be accessed
        search_string: str
            The string to be searched for
        size: int, optional
            The number of results to be returned.
             Defaults to 100 results

        Returns
        -------
        list:
            A list of headers which are close to the query. The queried value
            will be unique in the list so a search for piLast='Alice' will
            return only one header with Alice as the PI.

        Examples
        --------
        >>> db = Broker(...) # Contains runs from Bob, Alice, Bob, and Eve
        >>> fuzzy_set_search(db, 'bt_piLast', 'Bob')
        ['Bob', 'Alice', 'Eve']
        """
        heap = [(-1, -1)] * size  # ndld can't return less than 0
        heapify(heap)
        values = set([h['start'][key] for h in self()])
        for v in values:
            heappushpop(heap, (1. - ndld(v, search_string), v))
        heap.sort()
        heap.reverse()
        return [g[-1] for g in heap if g[0] >= 0.]


def _get_from_dict(data_dict, map_list):
    """Get a value from a nested dictionary, given a list of keys

    Parameters
    ----------
    data_dict: dict
        The dictionary to be queried
    map_list: list of str
        A list of strings, each string is one level lower than the previous

    Returns
    -------
    object:
        The the value from the dict

    """
    for k in map_list:
        data_dict = data_dict[k]
    return data_dict


def _nested_dict_values(d):
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
            yield from _nested_dict_values(v)
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
    heap = [(-1, -1, -1)] * size  # ndld can't return less than 0
    heapify(heap)
    if isinstance(keys, list):
        for h in db():
            # prioritize recent documents
            heappushpop(heap, (1. - ndld(_get_from_dict(h['start'], keys),
                                         search_string),
                               h['start']['time'] * -1, h))
    else:
        for h in db():
            heappushpop(heap, (1. - ndld(h['start'][keys], search_string),
                               h['start']['time'] * -1, h))
    heap.sort()
    heap.reverse()
    return [g[-1] for g in heap if g[0] >= 0.]


def super_fuzzy_search(db, search_string, size=100):
    """Fuzzy search a databroker

    Parameters
    ----------
    db: databroker.DataBroker instance
        The databroker to be searched
    search_string: str
        The string to be searched for
    size: int, optional
        The number of results to be returned.
         Defaults to 100 results

    Returns
    -------
    list:
        A list

    """
    heap = [(-1, -1, -1)] * size  # ndld can't return less than 0
    heapify(heap)
    for h in db():
        internal_scores = [1. - ndld(v, search_string) for v in
                           _nested_dict_values(h['start']) if v is not None]
        heappushpop(heap, (max(internal_scores), h['start']['time'] * -1, h))
    heap.sort()
    heap.reverse()
    return [g[-1] for g in heap if g[0] != -1]


def beamtime_dates(db, keys=('beamtime_uid', 'bt_safN',
                             'facility', 'beamline'),
                   beamtime_key='beamtime_uid',
                   print_results=True):
    """Get info for each beamtime

    Parameters
    ----------
    db: databroker instance
        The databroker to be searched
    keys: iterable of str
        The keys to be included in the return
    beamtime_key: str
        The key for the unique beamtime key
    print_results: bool
        If true prints the information

    Returns
    -------
    list of dicts:
        The list of beamtimes and their associated information
    """
    hdrs = db()
    bts = set([h['start'][beamtime_key] for h in hdrs])
    returns = []
    for s in bts:
        hdrs = db(**{beamtime_key: s})
        start_hdr = next(iter(hdrs))
        for hdr in hdrs:
            pass
        stop_hdr = hdr
        info = {k: start_hdr[k] for k in keys if k in start_hdr.keys()}
        info.update({'start_time': _munge_time(start_hdr['start']['time'],
                                               pytz.timezone('US/Eastern')),
                     'stop_time': _munge_time(stop_hdr['start']['time'],
                                              pytz.timezone('US/Eastern'))})
        returns.append(info)
    if print_results:
        pprint(returns)
    return returns


def fuzzy_set_search(db, key, search_string, size=100):
    """Return the most similar set of values to the search string.

    Parameters
    ----------
    db: databroker.DataBroker instance
        The databroker to be searched
    key: list of str
        The list of strings to be accessed
    search_string: str
        The string to be searched for
    size: int, optional
        The number of results to be returned.
         Defaults to 100 results

    Returns
    -------
    list:
        A list

    Examples
    --------
    >>> db = Broker(...) # Contains runs from Bob, Alice, Bob, and Eve
    >>> fuzzy_set_search(db, 'bt_piLast', 'Bob')
    ['Bob', 'Alice', 'Eve']
    """
    heap = [(-1, -1)] * size  # ndld can't return less than 0
    heapify(heap)
    values = set([h['start'][key] for h in db()])
    for v in values:
        heappushpop(heap, (1. - ndld(v, search_string), v))
    heap.sort()
    heap.reverse()
    return [g[-1] for g in heap if g[0] >= 0.]
