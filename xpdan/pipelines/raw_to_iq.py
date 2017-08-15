"""Example for XPD data"""
from operator import add, sub

import numpy as np
import shed.event_streams as es

from databroker import db
from skbeam.core.accumulators.binned_statistic import BinnedStatistic1D
from streams.core import Stream
from xpdan.db_utils import query_dark, query_background, temporal_prox
from xpdan.tools import better_mask_img


# def better_mask_img(geo, img, binner):
#     pass


def iq_to_pdf(stuff):
    pass


def refine_structure(stuff):
    pass


def LiveStructure(stuff):
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


def div(img, count):
    return img / count


def load_geo(cal_params):
    from pyFAI.azimuthalIntegrator import AzimuthalIntegrator
    ai = AzimuthalIntegrator()
    ai.setPyFAI(**cal_params)
    return ai


def event_count(x):
    return x['count'] + 1


source = Stream(name='Raw')

fg_dark_stream = es.QueryUnpacker(db, es.Query(db, source,
                                               query_function=query_dark,
                                               query_decider=temporal_prox,
                                               name='Query for FG Dark'))

bg_query_stream = es.Query(db, source,
                           query_function=query_background,
                           query_decider=temporal_prox,
                           name='Query for Background')

bg_stream = es.QueryUnpacker(db, bg_query_stream)
bg_dark_stream = es.QueryUnpacker(db, es.Query(db, bg_stream,
                                               query_function=query_dark,
                                               query_decider=temporal_prox,
                                               name='Query for BG Dark'))

# Perform dark subtraction on everything
dark_sub_bg = es.map((sub),
                     es.zip(bg_stream, bg_dark_stream),
                     input_info={'img1': ('pe1_image', 0),
                                 'img2': ('pe1_image', 1)},
                     output_info=[('img', {'dtype': 'array',
                                           'source': 'testing'})])

# bundle the backgrounds into one stream
bg_bundle = es.BundleSingleStream(dark_sub_bg, bg_query_stream,
                                  name='Background Bundle')

# sum the backgrounds
summed_bg = es.accumulate((add), bg_bundle, start=(pull_array),
                          state_key='img1',
                          input_info={'img2': 'img'},
                          output_info=[('img', {
                              'dtype': 'array',
                              'source': 'testing'})])

count_bg = es.accumulate(event_count, bg_bundle, start=1,
                         state_key='count',
                         output_info=[('count', {
                             'dtype': 'int',
                             'source': 'testing'})])

ave_bg = es.map((div), es.zip(summed_bg, count_bg),
                input_info={'img': ('img', 0), 'count': ('count', 1)},
                output_info=[('img', {
                    'dtype': 'array',
                    'source': 'testing'})],
                # name='Average Background'
                )

dark_sub_fg = es.map(sub,
                     es.zip(source,
                            fg_dark_stream),
                     input_info={'img1': ('pe1_image', 0),
                                 'img2': ('pe1_image', 1)},
                     output_info=[('img', {'dtype': 'array',
                                           'source': 'testing'})],
                     # name='Dark Subtracted Foreground'
                     )

# combine the fg with the summed_bg
fg_bg = es.combine_latest(dark_sub_fg, ave_bg, emit_on=dark_sub_fg)

# subtract the background images
fg_sub_bg = es.map(sub,
                   fg_bg,
                   input_info={'img1': ('img', 0),
                               'img2': ('img', 1)},
                   output_info=[('img', {'dtype': 'array',
                                         'source': 'testing'})],
                   # name='Background Corrected Foreground'
                   )

# make/get calibration stream
cal_md_stream = es.Eventify(source, start_key='calibration_md',
                            output_info=[('calibration_md',
                                          {'dtype': 'dict',
                                           'source': 'workflow'})],
                            md=dict(name='Calibration'))
cal_stream = es.map(load_geo, cal_md_stream,
                    input_info={'cal_params': 'calibration_md'},
                    output_info=[('geo',
                                  {'dtype': 'object', 'source': 'workflow'})])

# polarization correction
# SPLIT INTO TWO NODES
pfactor = .99
p_corrected_stream = es.map(polarization_correction,
                            es.lossless_combine_latest(fg_sub_bg, cal_stream),
                            input_info={'img': ('img', 0),
                                        'geo': ('geo', 1)},
                            output_info=[('img', {'dtype': 'array',
                                                  'source': 'testing'})],
                            polarization_factor=pfactor)

# generate masks
mask_kwargs = {'bs_width': None}
mask_stream = es.map(better_mask_img,
                     es.lossless_combine_latest(p_corrected_stream,
                                                cal_stream),
                     input_info={'img': ('img', 0),
                                 'geo': ('geo', 1)},
                     output_info=[('mask', {'dtype': 'array',
                                            'source': 'testing'})],
                     **mask_kwargs)

# generate binner stream
binner_stream = es.map(generate_binner,
                       cal_stream,
                       input_info={'geo': 'geo'},
                       output_info=[('binner', {'dtype': 'function',
                                                'source': 'testing'})],
                       img_shape=(2048, 2048))

iq_stream = es.map(integrate,
                   es.lossless_combine_latest(p_corrected_stream,
                                              binner_stream),
                   input_info={'img': ('img', 0),
                               'binner': ('binner', 1)},
                   output_info=[('iq', {'dtype': 'array',
                                        'source': 'testing'})])

# z-score the data
z_score_stream = es.map(z_score_image,
                        es.lossless_combine_latest(p_corrected_stream,
                                                   binner_stream),
                        input_info={'img': ('img', 0),
                                    'binner': ('binner', 1)},
                        output_info=[('z_score_img', {'dtype': 'array',
                                                      'source': 'testing'})])

pdf_stream = es.map(iq_to_pdf, es.zip(iq_stream, source))
