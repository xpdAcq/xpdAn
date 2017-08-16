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
import datetime
import scipy.stats as sts
from matplotlib.path import Path
from scipy.sparse import csr_matrix

# Ideally we would pull these functions from scikit-beam
from skbeam.core.accumulators.binned_statistic import BinnedStatistic1D

try:
    from skbeam.core.mask import margin, binned_outlier
# Otherwise we make them ourselves
except ImportError:
    def margin(img_shape, edge_size):
        """ Mask the edge of an image

        Parameters
        -----------
        img_shape: tuple
            The shape of the image.
            This is usually given by image.shape
        edge_size: int
            Number of pixels to mask from the edge

        Returns
        --------
        2darray:
            The mask array, bad pixels are 0
        """
        mask = np.ones(img_shape, dtype=bool)
        mask[edge_size:-edge_size, edge_size:-edge_size] = 0.
        return ~mask

    def binned_outlier(img, r, alpha, bins, mask=None):
        """ Generates a mask by identifying outlier pixels.

        The image is binned and any pixels which have a value greater or less
        than alpha * std away from the mean are masked.

        Parameters
        ----------
        img: 2darray
            The  image
        r: 2darray
            The array which maps pixels to bins.
            This is usually given by pyFAI.geometry.Geometry.rArray
        alpha: float or tuple or, 1darray
            The number of acceptable standard deviations.
            If tuple then we use a linear distribution of alphas from alpha[0]
            to alpha[1], if array then we use that as the distribution of
            alphas
        bins: list
            The bin edges
        mask: 1darray
            A starting flattened mask

        Returns
        --------
        2darray:
            The mask
        """

        if mask is None:
            working_mask = np.ones(img.shape).astype(bool)
        else:
            working_mask = mask.copy()
        msk_img = img[working_mask]
        msk_r = r[working_mask]

        int_r = np.digitize(r, bins[:-1], True) - 1
        # integration
        mean = sts.binned_statistic(msk_r, msk_img, bins=bins,
                                    statistic='mean')[0]
        std = sts.binned_statistic(msk_r, msk_img, bins=bins,
                                   statistic=np.std)[0]
        if type(alpha) is tuple:
            alpha = np.linspace(alpha[0], alpha[1], len(std))
        threshold = alpha * std
        lower = mean - threshold
        upper = mean + threshold

        # single out the too low and too high pixels
        working_mask *= img > lower[int_r]
        working_mask *= img < upper[int_r]

        return working_mask.astype(bool)


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
    img: ndarray
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
    tmsk: ndarray, optional
        The starting mask to be compounded on. Defaults to None. If None mask
        generated from scratch.

    Returns
    -------
    tmsk: ndarray
        The mask as a boolean array. True pixels are good pixels, False pixels
        are masked out.

    """

    r = geo.rArray(img.shape)
    pixel_size = [getattr(geo, a) for a in ['pixel1', 'pixel2']]
    rres = np.hypot(*pixel_size)
    rbins = np.arange(np.min(r) - rres / 2., np.max(r) + rres / 2., rres)
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
        working_mask *= binned_outlier(img, r, alpha, rbins, mask=working_mask)
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


def iq_to_pdf(stuff):
    pass


def pull_array(img2):
    return img2


def generate_binner(geo, img_shape, mask=None):
    r = geo.rArray(img_shape)
    q = geo.qArray(img_shape) / 10
    q_dq = geo.deltaQ(img_shape) / 10

    pixel_size = [getattr(geo, a) for a in ['pixel1', 'pixel2']]
    rres = np.hypot(*pixel_size)
    rbins = np.arange(np.min(r) - rres / 2., np.max(r) + rres / 2., rres / 2.)
    rbinned = BinnedStatistic1D(r.ravel(), statistic=np.max, bins=rbins, )

    qbin_sizes = rbinned(q_dq.ravel())
    qbin_sizes = np.nan_to_num(qbin_sizes)
    qbin = np.cumsum(qbin_sizes)
    if mask:
        mask = mask.flatten()
    return BinnedStatistic1D(q.flatten(), bins=qbin, mask=mask)


def z_score_image(img, binner):
    img_shape = img.shape
    img = img.flatten()
    xy = binner.xy
    binner.statistic = 'mean'
    means = binner(img)
    binner.statistic = 'std'
    stds = binner(img)
    for i in np.unique(xy):
        tv = (xy == i)
        img[tv] -= means[i]
        img[tv] /= stds[i]
    img = img.reshape(img_shape)
    return img


def integrate(img, binner):
    return binner.bin_centers, binner(img.flatten())


def polarization_correction(img, geo, polarization_factor=.99):
    return img / geo.polarization(img.shape, polarization_factor)


def load_geo(cal_params):
    from pyFAI.azimuthalIntegrator import AzimuthalIntegrator
    ai = AzimuthalIntegrator()
    ai.setPyFAI(**cal_params)
    return ai


def event_count(x):
    return x['count'] + 1

def _timestampstr(timestamp):
    """convert timestamp to strftime formate"""
    timestring = datetime.datetime.fromtimestamp(float(timestamp)).strftime(
        '%Y%m%d-%H%M')
    return timestring
