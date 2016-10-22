import numpy as np
import os
from xpdan.io import fit2d_save, read_fit2d_msk
from numpy.testing import assert_array_equal


def test_save_output_fit2d(mk_glbl):
    filename = os.path.join(mk_glbl.base, "function_values")
    msk = np.random.random_integers(
        0, 1, (np.random.random_integers(0, 200),
               np.random.random_integers(0, 200))).astype(bool)

    fit2d_save(msk, filename, dir_path=None)
    msk2 = read_fit2d_msk(filename+'.msk')
    assert_array_equal(msk2, msk)

    os.remove(filename+'.msk')
