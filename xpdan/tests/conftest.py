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
import tempfile
import uuid
import pytest
import numpy as np

from xpdsim import xpd_pe1c as det
from bluesky.tests.conftest import RE as fresh_RE, db, NumpySeqHandler
from skbeam.io.fit2d import fit2d_save

from xpdan.fuzzybroker import FuzzyBroker
from xpdan.glbl_gen import make_glbl, load_configuration
from .utils import insert_imgs


if sys.version_info >= (3, 0):
    pass


@pytest.fixture(scope='function')
def start_uid3(exp_db):
    assert 'start_uid3' in exp_db[6]['start']
    return str(exp_db[6]['start']['uid'])


@pytest.fixture(scope='module')
def img_size():
    # a = np.random.random_integers(100, 200)
    a = 2048
    yield (a, a)


@pytest.fixture(scope='module')
def ltdb(request):
    """Return a data broker
    """
    from databroker.tests.utils import build_sqlite_backed_broker
    db = build_sqlite_backed_broker(request)
    return db


@pytest.fixture(scope='module')
def exp_db(ltdb, tmpdir, img_size, fresh_RE):
    db2 = ltdb
    reg = db2.reg
    db.reg.register_handler('NPY_SEQ', NumpySeqHandler)
    RE = fresh_RE
    RE.subscribe(db.insert)
    bt_uid = str(uuid.uuid4)

    insert_imgs(RE, reg, 2, img_size, tmpdir, bt_safN=0, pi_name='chris',
                sample_name='kapton', sample_composition='C', start_uid1=True,
                bt_uid=bt_uid, composition_string='Au')
    insert_imgs(RE, reg, 2, img_size, tmpdir, pi_name='tim', bt_safN=1,
                sample_name='Au', bkgd_sample_name='kapton',
                sample_composition='Au', start_uid2=True, bt_uid=bt_uid,
                composition_string='Au')
    insert_imgs(RE, reg, 2, img_size, tmpdir, pi_name='chris', bt_safN=2,
                sample_name='Au', bkgd_sample_name='kapton',
                sample_composition='Au', start_uid3=True, bt_uid=bt_uid,
                composition_string='Au')
    yield db2


@pytest.fixture(scope='function')
def fuzzdb(exp_db):
    yield FuzzyBroker(exp_db.mds, exp_db.reg)


@pytest.fixture(scope='function')
def fast_tmp_dir():
    td = tempfile.TemporaryDirectory()
    print('creating {}'.format(td.name))
    yield td.name
    if os.path.exists(td.name):
        print('removing {}'.format(td.name))
        td.cleanup()
