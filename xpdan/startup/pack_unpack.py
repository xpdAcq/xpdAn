# couple = 'pickle'
# couple = 'msgpack'
couple = 'ujson'

if couple == 'pickle':
    import pickle

    serializer = pickle.dumps
    deserializer = pickle.loads

elif couple == 'msgpack':
    import msgpack
    import msgpack_numpy as m
    m.patch()

    serializer = msgpack.dumps


    def convert(data):
        if isinstance(data, bytes):
            return data.decode('ascii')
        if isinstance(data, dict):
            return dict(map(convert, data.items()))
        if isinstance(data, tuple):
            return tuple(map(convert, data))
        if isinstance(data, list):
            return list(map(convert, data))
        return data


    # Magic number comes from face planting, needs more investigation
    deserializer = lambda x: convert(msgpack.loads(x,
                                                   max_str_len=16777216+1000))
