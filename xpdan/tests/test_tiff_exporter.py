import os
import tempfile
from itertools import product
from tifffile import imread
from xpdan.tests.conftest import img_size
from xpdan.callbacks_core import XpdAcqLiveTiffExporter

# standard config
base = tempfile.mkdtemp()
template = os.path.join(base, 'xpdUser/tiff_base/')
data_fields = ['temperature', 'diff_x', 'diff_y', 'eurotherm'] # known devices

# function options
good_params= ['save_dark', 'dry_run', 'overwrite']
allowed_kwargs = [(True, False), (True, False), (True, False)]
bad_params = ['save_dark', 'dry_run', 'overwrite']
fail_kwargs = [['fail'] for i in range(len(allowed_kwargs))]

# parametrize
param_testing_list = []
kwargs_product = product(*allowed_kwargs)

for el in kwargs_product:
    d = {k:v for k,v in zip(good_params, el)}
    param_testing_list.append(d)

for el in fail_kwargs:
    d = {k:v for k,v in zip(good_params, el)}
    param_testing_list.append(d)

def test_tiff_export(exp_db):
    tif_export = XpdAcqLiveTiffExporter('pe1_image', template, data_fields,
                                        overwrite=True, db=exp_db)
    exp_db.process(exp_db[-1], tif_export)
    # make sure files are sasved
    for fn in tif_export.filenames:
        assert os.path.isfile(fn)
    # confirm image is the same as input
    for fn in tif_export.filenames:
        img = imread(fn)
        assert img.shape == next(img_size)
        # logic defined in insert_img. after successful dark_sub array==0
        # TODO: update this logic when we are ready for
        # fs-integrated-Reader
        assert np.all(img == 0)
