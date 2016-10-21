import shutil
import sys
import uuid
import tempfile
import os

import pytest
from databroker import Broker
from filestore.handlers import NpyHandler

from xpdan.tests.utils import insert_imgs
from xpdan.data_reduction import DataReduction
from xpdan.glbl import make_glbl
from xpdan.simulation import build_pymongo_backed_broker
from skbeam.io.fit2d_save import fit2d_save
import numpy as np

if sys.version_info >= (3, 0):
    pass


@pytest.fixture(scope='module')
def mk_glbl():
    a = make_glbl(1)
    yield a
    if os.path.exists(a.base):
        print('removing {}'.format(a.base))
        shutil.rmtree(a.base)


@pytest.fixture(params=[
    # 'sqlite',
    'mongo'], scope='module')
def db(request):
    param_map = {
        # 'sqlite': build_sqlite_backed_broker,
        'mongo': build_pymongo_backed_broker}

    return param_map[request.param](request)


@pytest.fixture(scope='module')
def handler(exp_db):
    h = DataReduction(exp_db=exp_db)
    return h


@pytest.fixture(scope='module')
def exp_db(db, mk_glbl):
    glbl = mk_glbl
    db2 = db
    mds = db2.mds
    fs = db2.fs
    insert_imgs(mds, fs, 5, (200, 200), glbl.base)
    yield db2
    print("DROPPING MDS")
    mds._connection.drop_database(mds.config['database'])
    print("DROPPING FS")
    fs._connection.drop_database(fs.config['database'])


@pytest.fixture(scope='module')
def disk_mask(mk_glbl):
    mask = np.random.random_integers(0, 1, (200, 200)).astype(bool)
    dirn = mk_glbl.base
    file_name = os.path.join(dirn, 'mask_test' + '.msk')
    assert ~os.path.exists(file_name)
    fit2d_save(mask, 'mask_test', dirn)
    assert os.path.exists(file_name)
    yield (file_name, mask)
