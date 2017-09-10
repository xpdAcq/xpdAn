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
import numpy as np

try:
    from diffpy.pdfgetx import PDFGetter
except ImportError:
    from xpdan.tests.utils import PDFGetterShim as PDFGetter
from matplotlib.path import Path
from scipy.sparse import csr_matrix
from multiprocessing.dummy import Pool

from skbeam.core.accumulators.binned_statistic import BinnedStatistic1D
from skbeam.core.mask import margin, binned_outlier


# TODO: speed this up
def new_masking_method(img, geo, alpha=3, tmsk=None):
    """Sigma Clipping based masking

    Parameters
    ----------
    img: np.ndarray
        The image
    geo: pyFAI.geometry.Geometry instance
        The detector geometry information
    alpha: float
        The number of standard deviations to clip
    tmsk: np.ndarray, optional
        Prior mask. If None don't use a prior mask, defaults to None.

    Returns
    -------
    np.ndarray:
        The mask
    """
    print('start mask')
    qbinned = generate_binner(geo, img.shape)
    xy = qbinned.xy

    ripos = np.arange(0, np.size(img))
    rimg = img.flatten()

    if tmsk is None:
        tmsk = np.ones(img.shape)

    # TODO: speed this up
    def mask_ring(i):
        """Find outlier pixels in a single ring """
        tv = (xy == i) & tmsk.flatten()

        values_array = rimg[tv]
        positions_array = ripos[tv]
        while len(values_array) > 0:
            norm_v_list = np.abs(values_array -
                                 np.mean(values_array)) / np.std(values_array)
            if np.all(norm_v_list < alpha):
                break
            # get the index of the worst pixel
            worst_idx = np.argmax(norm_v_list)
            # add the worst position to the mask
            tmsk[np.unravel_index(positions_array[worst_idx],
                                  img.shape)] = False
            # delete the worst position
            values_array = np.delete(values_array, worst_idx)
            positions_array = np.delete(positions_array, worst_idx)

    with Pool() as p:
        [_ for _ in p.imap_unordered(mask_ring, np.unique(xy), 10)]
    print('finished mask')
    return tmsk.astype(bool)


def old_mask_img(img, geo,
                 edge=30,
                 lower_thresh=0.0,
                 upper_thresh=None,
                 bs_width=13, tri_offset=13, v_asym=0,
                 alpha=2.5,
                 tmsk=None):
    """
    Mask an image based off of various methods

    Parameters
    ----------
    img: np.ndarray
        The image to be masked
    geo: pyFAI.geometry.Geometry
        The pyFAI description of the detector orientation or any
        subclass of pyFAI.geometry.Geometry class
    edge: int, optional
        The number of edge pixels to mask. Defaults to 30. If None, no edge
        mask is applied
    lower_thresh: float, optional
        Pixels with values less than or equal to this threshold will be masked.
        Defaults to 0.0. If None, no lower threshold mask is applied
    upper_thresh: float, optional
        Pixels with values greater than or equal to this threshold will be
        masked.
        Defaults to None. If None, no upper threshold mask is applied.
    bs_width: int, optional
        The width of the beamstop in pixels. Defaults to 13.
        If None, no beamstop polygon mask is applied.
    tri_offset: int, optional
        The triangular pixel offset to create a pointed beamstop polygon mask.
        Defaults to 13. If None, no beamstop polygon mask is applied.
    v_asym: int, optional
        The vertical asymmetry of the polygon beamstop mask. Defaults to 0.
        If None, no beamstop polygon mask is applied.
    alpha: float or tuple or, 1darray, optional
        Then number of acceptable standard deviations, if tuple then we use
        a linear distribution of alphas from alpha[0] to alpha[1], if array
        then we just use that as the distribution of alphas. Defaults to 2.5.
        If None, no outlier masking applied.
    tmsk: np.ndarray, optional
        The starting mask to be compounded on. Defaults to None. If None mask
        generated from scratch.

    Returns
    -------
    tmsk: np.ndarray
        The mask as a boolean array. True pixels are good pixels, False pixels
        are masked out.

    """
    q = geo.qArray(img.shape)
    qbinned = generate_binner(geo, img.shape)
    if tmsk is None:
        working_mask = np.ones(img.shape).astype(bool)
    else:
        working_mask = tmsk.copy()
    if edge:
        working_mask *= margin(img.shape, edge)
    if lower_thresh:
        working_mask *= (img >= lower_thresh).astype(bool)
    if upper_thresh:
        working_mask *= (img <= upper_thresh).astype(bool)
    if all([a is not None for a in [bs_width, tri_offset, v_asym]]):
        center_x, center_y = [geo.getFit2D()[k] for k in
                              ['centerX', 'centerY']]
        nx, ny = img.shape
        mask_verts = [(center_x - bs_width, center_y),
                      (center_x, center_y - tri_offset),
                      (center_x + bs_width, center_y),
                      (center_x + bs_width + v_asym, ny),
                      (center_x - bs_width - v_asym, ny)]

        x, y = np.meshgrid(np.arange(nx), np.arange(ny))
        x, y = x.flatten(), y.flatten()

        points = np.vstack((x, y)).T

        path = Path(mask_verts)
        grid = path.contains_points(points)
        # Plug msk_grid into into next (edge-mask) step in automask
        working_mask *= ~grid.reshape((ny, nx))

    if alpha:
        working_mask *= binned_outlier(img, q, alpha, qbinned.bin_edges,
                                       mask=working_mask)
    return working_mask


def mask_img(img, geo,
             edge=30,
             lower_thresh=0.0,
             upper_thresh=None,
             bs_width=13, tri_offset=13, v_asym=0,
             alpha=2.5,
             tmsk=None):
    """
    Mask an image based off of various methods

    Parameters
    ----------
    img: np.ndarray
        The image to be masked
    geo: pyFAI.geometry.Geometry
        The pyFAI description of the detector orientation or any
        subclass of pyFAI.geometry.Geometry class
    edge: int, optional
        The number of edge pixels to mask. Defaults to 30. If None, no edge
        mask is applied
    lower_thresh: float, optional
        Pixels with values less than or equal to this threshold will be masked.
        Defaults to 0.0. If None, no lower threshold mask is applied
    upper_thresh: float, optional
        Pixels with values greater than or equal to this threshold will be
        masked.
        Defaults to None. If None, no upper threshold mask is applied.
    bs_width: int, optional
        The width of the beamstop in pixels. Defaults to 13.
        If None, no beamstop polygon mask is applied.
    tri_offset: int, optional
        The triangular pixel offset to create a pointed beamstop polygon mask.
        Defaults to 13. If None, no beamstop polygon mask is applied.
    v_asym: int, optional
        The vertical asymmetry of the polygon beamstop mask. Defaults to 0.
        If None, no beamstop polygon mask is applied.
    alpha: float or tuple or, 1darray, optional
        Then number of acceptable standard deviations, if tuple then we use
        a linear distribution of alphas from alpha[0] to alpha[1], if array
        then we just use that as the distribution of alphas. Defaults to 2.5.
        If None, no outlier masking applied.
    tmsk: np.ndarray, optional
        The starting mask to be compounded on. Defaults to None. If None mask
        generated from scratch.

    Returns
    -------
    tmsk: np.ndarray
        The mask as a boolean array. True pixels are good pixels, False pixels
        are masked out.

    """

    if tmsk is None:
        working_mask = np.ones(img.shape).astype(bool)
    else:
        working_mask = tmsk.copy()
    if edge:
        working_mask *= margin(img.shape, edge)
    if lower_thresh:
        working_mask *= (img >= lower_thresh).astype(bool)
    if upper_thresh:
        working_mask *= (img <= upper_thresh).astype(bool)
    if all([a is not None for a in [bs_width, tri_offset, v_asym]]):
        center_x, center_y = [geo.getFit2D()[k] for k in
                              ['centerX', 'centerY']]
        nx, ny = img.shape
        mask_verts = [(center_x - bs_width, center_y),
                      (center_x, center_y - tri_offset),
                      (center_x + bs_width, center_y),
                      (center_x + bs_width + v_asym, ny),
                      (center_x - bs_width - v_asym, ny)]

        x, y = np.meshgrid(np.arange(nx), np.arange(ny))
        x, y = x.flatten(), y.flatten()

        points = np.vstack((x, y)).T

        path = Path(mask_verts)
        grid = path.contains_points(points)
        # Plug msk_grid into into next (edge-mask) step in automask
        working_mask *= ~grid.reshape((ny, nx))

    if alpha:
        working_mask *= new_masking_method(img, geo, alpha=alpha,
                                           tmsk=working_mask)
    working_mask = working_mask.astype(np.bool)
    return working_mask


def compress_mask(mask):
    """Compress a mask via a csr sparse matrix

    Parameters
    ----------
    mask: 2d boolean array
        The mask, True/1 are good pixels, False/0 are bad

    Returns
    -------
    list:
        The csr_matrix data
    list:
        The csr_matrix indices
    list:
        The csr_matrix indptr

    See Also:
    ---------
    scipy.sparse.csr_matrix
    """
    cmask = csr_matrix(~mask)
    # FIXME: we may need to also return the mask shape
    return cmask.data.tolist(), cmask.indices.tolist(), cmask.indptr.tolist()


def decompress_mask(data, indices, indptr, shape):
    """Decompress a csr sparse matrix into a mask

    Parameters
    ----------
    data: list
        The csr_matrix data
    indices: list
        The csr_matrix indices
    indptr: list
        The csr_matrix indptr
    shape: tuple
        The shape of the array to be recreated

    Returns
    -------
    mask: 2d boolean array
        The mask, True/1 are good pixels, False/0 are bad

    See Also:
    ---------
    scipy.sparse.csr_matrix
    """
    cmask = csr_matrix(
        tuple([np.asarray(a) for a in [data, indices, indptr]]), shape=shape)
    return ~cmask.toarray().astype(bool)


def pull_array(img2):
    return img2


def generate_binner(geo, img_shape, mask=None):
    r = geo.rArray(img_shape)
    q = geo.qArray(img_shape) / 10  # type: np.ndarray
    q_dq = geo.deltaQ(img_shape) / 10  # type: np.ndarray

    pixel_size = [getattr(geo, a) for a in ['pixel1', 'pixel2']]
    rres = np.hypot(*pixel_size)
    rbins = np.arange(np.min(r) - rres / 2., np.max(r) + rres / 2., rres / 2.)
    # This is only called once, use the function version
    rbinned = BinnedStatistic1D(r.ravel(), statistic=np.max, bins=rbins, )

    qbin_sizes = rbinned(q_dq.ravel())
    qbin_sizes = np.nan_to_num(qbin_sizes)
    qbin = np.cumsum(qbin_sizes)
    if mask is not None:
        mask = mask.flatten()
    return BinnedStatistic1D(q.flatten(), bins=qbin, mask=mask)


def z_score_image(img, binner):
    img2 = img.flatten()
    xy = binner.xy

    means = binner(img, 'mean')

    stds = binner(img, 'std')

    def f(i):
        tv = (xy == i)
        img2[tv] -= means[i]
        img2[tv] /= stds[i]

    with Pool() as p:
        p.map(f, np.unique(xy))

    return img2.reshape(img.shape)


def integrate(img, binner):
    return binner.bin_centers, np.nan_to_num(binner(img.flatten()))


def polarization_correction(img, geo, polarization_factor=.99):
    return img / geo.polarization(img.shape, polarization_factor)


def load_geo(cal_params):
    from pyFAI.azimuthalIntegrator import AzimuthalIntegrator
    ai = AzimuthalIntegrator()
    ai.setPyFAI(**cal_params)
    return ai


def event_count(x):
    return x['count'] + 1


def add_img(img1, img2):
    # Note that this exists because accumulate doesn't take args yet
    return img1 + img2


def pdf_getter(*args, **kwargs):
    pg = PDFGetter()
    res = pg(*args, **kwargs)
    return res[0], res[1], pg.config


def fq_getter(*args, **kwargs):
    pg = PDFGetter()
    pg(*args, **kwargs)
    res = pg.fq
    return res[0], res[1], pg.config
