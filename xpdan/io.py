import fabio


def read_msk(filename):
    a = fabio.open(filename)
    return ~a
