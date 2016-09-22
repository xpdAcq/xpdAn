import numpy as np
from pyFAI.geometry import Geometry
from xpdan.tools import margin, binned_outlier
from numpy.testing import assert_array_equal


def test_margin():
    size = (10, 10)
    edge = 1
    mask1 = margin(size, edge)
    mask2 = np.zeros(size)
    mask2[:, :edge] = 1
    mask2[:, -edge:] = 1
    mask2[:edge, :] = 1
    mask2[-edge:, :] = 1
    mask2 = mask2.astype(bool)
    assert_array_equal(mask1, ~mask2)


def test_ring_blur_mask():
    from skbeam.core import recip
    g = recip.geo.Geometry(
        detector='Perkin', pixel1=.0002, pixel2=.0002,
        dist=.23,
        poni1=.209, poni2=.207,
        # rot1=.0128, rot2=-.015, rot3=-5.2e-8,
        wavelength=1.43e-11
    )
    r = g.rArray((2048, 2048))
    # make some sample data
    Z = 100 * np.cos(50 * r) ** 2 + 150

    np.random.seed(10)
    pixels = []
    for i in range(0, 100):
        a, b = np.random.randint(low=0, high=2048), \
               np.random.randint(low=0, high=2048)
        if np.random.random() > .5:
            # Add some hot pixels
            Z[a, b] = np.random.randint(low=200, high=255)
        else:
            # and dead pixels
            Z[a, b] = np.random.randint(low=0, high=10)
        pixels.append((a, b))
    pixel_size = [getattr(g, k) for k in ['pixel1', 'pixel2']]
    rres = np.hypot(*pixel_size)
    bins = np.arange(np.min(r) - rres / 2., np.max(r) + rres / 2., rres)
    msk = binned_outlier(Z, r, (3., 3), bins, mask=None)
    a = set(zip(*np.nonzero(~msk)))
    b = set(pixels)
    a_not_in_b = a - b
    b_not_in_a = b - a

    # We have not over masked 10% of the number of bad pixels
    assert len(a_not_in_b) / len(b) < .1
    # Make certain that we have masked over 90% of the bad pixels
    assert len(b_not_in_a) / len(b) < .1
