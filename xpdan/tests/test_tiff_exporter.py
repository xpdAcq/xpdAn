##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Timothy Liu
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################

import os
from itertools import product

import numpy as np
import pytest
from numpy.testing import assert_array_equal
from tifffile import imread

from xpdan.callbacks_core import XpdAcqLiveTiffExporter

# standard config
data_fields = ['temperature', 'diff_x', 'diff_y', 'eurotherm']  # known devices

# function options
good_params = ['save_dark']
allowed_kwargs = [(True, False), (True, False), (True, False)]
# bad_params = ['save_dark', 'dryrun', 'overwrite']
# fail_kwargs = [['fail'] for i in range(len(allowed_kwargs))]

# parametrize
test_kwargs = []
allowed_kwargs_values = product(*allowed_kwargs)

for el in allowed_kwargs_values:
    d = {k: v for k, v in zip(good_params, el)}
    test_kwargs.append((d, False))


@pytest.mark.parametrize(("kwargs", "known_fail_bool"), test_kwargs)
def test_tiff_export(exp_db, tif_exporter_template, img_size,
                     kwargs, known_fail_bool):
    tif_export = XpdAcqLiveTiffExporter('pe1_image', tif_exporter_template,
                                        data_fields, overwrite=True,
                                        db=exp_db, **kwargs)
    a = exp_db.process(exp_db[-1], tif_export)
    # make sure files are saved
    assert len(tif_export.filenames) != 0
    for fn in tif_export.filenames:
        assert os.path.isfile(fn)
    # confirm image is the same as input
    dark_fn = [fn for fn in tif_export.filenames if
               fn.startswith('dark')]
    # light_fn = list(set(tif_export.filenames) - set(dark_fn))
    for fn in dark_fn:
        img = imread(fn)
        assert img.shape == img_size
        # Need to fix the schema first
        # assert np.all(img == 1)

    for in_img, fn in zip(exp_db.get_images(exp_db[-1], 'pe1_image'),
                          tif_export.filenames):
        img = imread(fn)
        assert img.shape == img_size
        # logic defined in insert_img. after successful dark_sub array==0
        print(kwargs)
        assert_array_equal(img, np.subtract(in_img, tif_export.dark_img))
        # TODO: update this logic when we are ready for db integrated

    if known_fail_bool and not a:
        pytest.xfail('Bad params')
