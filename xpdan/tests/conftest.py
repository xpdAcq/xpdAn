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
from bluesky.tests.conftest import db, NumpySeqHandler, RunEngine, asyncio
from skbeam.io.fit2d import fit2d_save

from xpdan.fuzzybroker import FuzzyBroker
from .utils import insert_imgs


if sys.version_info >= (3, 0):
    pass


@pytest.fixture(scope='module')
def fresh_RE(request):
    loop = asyncio.new_event_loop()
    loop.set_debug(True)
    RE = RunEngine({}, loop=loop)
    RE.ignore_callback_exceptions = False

    def clean_event_loop():
        if RE.state != 'idle':
            RE.halt()
        ev = asyncio.Event(loop=loop)
        ev.set()
        loop.run_until_complete(ev.wait())

    request.addfinalizer(clean_event_loop)
    return RE


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
def exp_db(ltdb, tmp_dir, img_size, fresh_RE):
    db2 = ltdb
    reg = db2.reg
    reg.register_handler('NPY_SEQ', NumpySeqHandler)
    RE = fresh_RE
    RE.subscribe(db2.insert)
    bt_uid = str(uuid.uuid4)

    insert_imgs(RE, reg, 2, img_size, tmp_dir, bt_safN=0, pi_name='chris',
                sample_name='kapton', sample_composition='C', start_uid1=True,
                bt_uid=bt_uid, composition_string='Au')
    insert_imgs(RE, reg, 2, img_size, tmp_dir, pi_name='tim', bt_safN=1,
                sample_name='Au', bkgd_sample_name='kapton',
                sample_composition='Au', start_uid2=True, bt_uid=bt_uid,
                composition_string='Au')
    insert_imgs(RE, reg, 2, img_size, tmp_dir, pi_name='chris', bt_safN=2,
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


@pytest.fixture(scope='module')
def tmp_dir():
    td = tempfile.TemporaryDirectory()
    print('creating {}'.format(td.name))
    yield td.name
    if os.path.exists(td.name):
        print('removing {}'.format(td.name))
        td.cleanup()


@pytest.fixture(scope='module')
def test_md():
    return {'analysis_stage': 'raw',
            'beamline_config': {},
            'beamline_id': '28-ID-2',
            'bt_experimenters': ['Long', 'Yang', 'Elizabeth', 'Culbertson'],
            'bt_piLast': 'Billinge',
            'bt_safN': '301750',
            'bt_uid': '34f53730',
            'bt_wavelength': 0.1867,
            'composition_string': 'C18.0H14.0',
            'detector_calibration_client_uid': '57d48021-d6a3-4b98-99ac'
                                               '-dfc9ba4cdff2',
            'detectors': ['pe1'],
            'facility': 'NSLS-II',
            'group': 'XPD',
            'hints': {'dimensions': [['time', 'primary']]},
            'lead_experimenter': ['Elizabeth'],
            'mask_client_uid': '57d48021-d6a3-4b98-99ac-dfc9ba4cdff2',
            'num_intervals': 0,
            'num_points': 1,
            'plan_args': {
                'detectors': [
                    "PerkinElmerContinuous(prefix='XF:28IDC-ES:1{Det:PE1}', "
                    "name='pe1', read_attrs=['tiff', 'stats1.total'], "
                    "configuration_attrs=['cam', 'images_per_set', "
                    "'number_of_sets'])"],
                'num': 1},
            'plan_name': 'count',
            'plan_type': 'generator',
            'sa_uid': '67043ebb',
            'sample_composition': {'C': 18.0, 'H': 14.0},
            'sample_name': 'undoped_ptp',
            'sample_phase': {'C18H14': 1.0},
            'sc_dk_field_uid': 'ad7be0bf-b52e-44f6-ad99-9fb330414df2',
            'scan_id': 980,
            'sp_computed_exposure': 120.0,
            'sp_num_frames': 60.0,
            'sp_plan_name': 'ct',
            'sp_requested_exposure': 120,
            'sp_time_per_frame': 2.5,
            'sp_type': 'ct',
            'sp_uid': '6da5d267-257a-44e8-9b7c-068d08ab7f68',
            'time': 1508919212.3547237,
            'uid': '14c5fe8a-0462-4df4-8440-f738ccd83380',
            'xpdacq_md_version': 0.1}
