from xpdan.io import read_msk
from numpy.testing import assert_array_equal


def test_read_msk(disk_mask):
    fn, mask = disk_mask
    mask2 = read_msk(fn)
    assert_array_equal(mask2, mask)
