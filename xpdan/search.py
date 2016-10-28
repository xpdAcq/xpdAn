from pyxdameraulevenshtein import \
    normalized_damerau_levenshtein_distance as ndld


def getFromDict(dataDict, mapList):
    for k in mapList:
        dataDict = dataDict[k]
    return dataDict


def nested_dict_values(d):
    for v in d.values():
        if isinstance(v, dict):
            yield from nested_dict_values(v)
        else:
            if isinstance(v, str):
                yield v
            else:
                yield None


def fuzzy_search(db, keys, search_string, size=100):
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
    return hdrs[:size]
