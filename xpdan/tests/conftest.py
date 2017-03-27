##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Timothy Liu, Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
import os
import shutil
import sys

import numpy as np
import pytest
import tempfile

# from xpdan.data_reduction import DataReduction
from xpdan.glbl_gen import make_glbl, load_configuration
from xpdan.io import fit2d_save
from xpdan.simulation import build_pymongo_backed_broker
from xpdan.tests.utils import insert_imgs
from xpdan.fuzzybroker import FuzzyBroker
import tempfile

if sys.version_info >= (3, 0):
    pass


def clean_database(database):
    for sub_db_name in ['mds', 'fs']:
        sub_db = getattr(database, sub_db_name)
        sub_db._connection.drop_database(sub_db.config['database'])


@pytest.fixture(scope='module')
def img_size():
    a = np.random.random_integers(100, 200)
    yield (a, a)


@pytest.fixture(scope='module')
def mk_glbl(exp_db):
    a = make_glbl(load_configuration('xpdan'), 1, exp_db)
    yield a
    if os.path.exists(a['base_dir']):
        print('removing {}'.format(a['base_dir']))
        shutil.rmtree(a['base_dir'])


@pytest.fixture(params=[
    # 'sqlite',
    'mongo'], scope='module')
def db(request):
    print('Making DB')
    param_map = {
        # 'sqlite': build_sqlite_backed_broker,
        'mongo': build_pymongo_backed_broker}
    rv = param_map[request.param](request)
    yield rv
    clean_database(rv)


@pytest.fixture(scope='module')
def tif_exporter_template():
    base = tempfile.mkdtemp()
    export_dir_template = os.path.join(base, 'xpdUser/tiff_base/')
    yield export_dir_template
    if os.path.isdir(export_dir_template):
        print('tearing {}'.format(export_dir_template))
        shutil.rmtree(export_dir_template)


@pytest.fixture(scope='module')
def handler(exp_db):
    h = DataReduction(exp_db=exp_db)
    return h


@pytest.fixture(scope='module')
def exp_db(db, tmp_dir, img_size):
    db2 = db
    mds = db2.mds
    fs = db2.fs
    insert_imgs(mds, fs, 5, img_size, tmp_dir, bt_safN=0, pi_name='chris')
    insert_imgs(mds, fs, 5, img_size, tmp_dir, pi_name='tim', bt_safN=1)
    insert_imgs(mds, fs, 5, img_size, tmp_dir, pi_name='chris', bt_safN=2)
    yield db2


@pytest.fixture(scope='module')
def disk_mask(tmp_dir, img_size):
    mask = np.random.random_integers(0, 1, img_size).astype(bool)
    dirn = tmp_dir
    file_name_msk = os.path.join(dirn, 'mask_test' + '.msk')
    assert ~os.path.exists(file_name_msk)
    fit2d_save(mask, 'mask_test', dirn)
    assert os.path.exists(file_name_msk)
    file_name = os.path.join(dirn, 'mask_test' + '.npy')
    assert ~os.path.exists(file_name)
    np.save(file_name, mask)
    assert os.path.exists(file_name)
    yield (file_name_msk, file_name, mask)


@pytest.fixture(scope='module')
def fuzzdb(exp_db):
    yield FuzzyBroker(exp_db.mds, exp_db.fs)


@pytest.fixture(scope='module')
def tmp_dir():
    td = tempfile.mkdtemp()
    print('creating {}'.format(td))
    yield td
    if os.path.exists(td):
        print('removing {}'.format(td))
        shutil.rmtree(td)


@pytest.fixture(scope='function')
def fast_tmp_dir():
    td = tempfile.mkdtemp()
    print('creating {}'.format(td))
    yield td
    if os.path.exists(td):
        print('removing {}'.format(td))
        shutil.rmtree(td)
