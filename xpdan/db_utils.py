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
    verbose: bool
        If true prints the results

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
