import os
import socket
import yaml
import time
from time import strftime
import tempfile
import shutil
from mock import MagicMock

import matplotlib
matplotlib.use('qt4agg')
import tempfile
import os
import tzlocal

HOME_DIR_NAME = 'xpdUser'
BLCONFIG_DIR_NAME = 'xpdConfig'
BEAMLINE_HOST_NAME = 'xf28id1-ws2'
ARCHIVE_BASE_DIR_NAME = 'pe2_data/.userBeamtimeArchive'
USER_BACKUP_DIR_NAME = strftime('%Y')
OWNER = 'xf28id1'
BEAMLINE_ID = 'xpd'
GROUP = 'XPD'
DET_IMAGE_FIELD = 'pe1_image'
DARK_FIELD_KEY = 'sc_dk_field_uid'
CALIB_CONFIG_NAME = 'pyFAI_calib.yml'


# make db for simulation
def make_broker():
    from portable_mds.sqlite.mds import MDS
    from portable_fs.sqlite.fs import FileStore
    from databroker import Broker

    # make visible temp_dir, a layer up than xpdUser
    mds_dir = tempfile.mkdtemp()
    mds = MDS({'directory': mds_dir,
               'timezone': tzlocal.get_localzone().zone})
    fs = FileStore({'dbpath': os.path.join(mds_dir, 'filestore.db')})
    return Broker(mds, fs)


def make_glbl(env_code=0):
    """ make a instance of Glbl class

    Glbl class is used to handle attributes and directories
     depends on environment variable

    Parameters
    ----------
    en_var : int
        environment variable to specify current situation

    Note
    ----
    by default: env_var 0 means beamline, 1 means test, 2 means simulation
    """

    HOME_DIR_NAME = 'xpdUser'
    BLCONFIG_DIR_NAME = 'xpdConfig'
    BEAMLINE_HOST_NAME = 'xf28id1-ws2'
    ARCHIVE_BASE_DIR_NAME = 'pe2_data/.userBeamtimeArchive'
    USER_BACKUP_DIR_NAME = strftime('%Y')
    OWNER = 'xf28id1'
    BEAMLINE_ID = 'xpd'
    GROUP = 'XPD'
    DET_IMAGE_FIELD = 'pe1_image'
    DARK_FIELD_KEY = 'sc_dk_field_uid'
    CALIB_CONFIG_NAME = 'pyFAI_calib.yml'

    # change this to be handled by an environment variable later
    # test
    if int(env_code) == 1:
        from databroker import db # import db created for test
        BASE_DIR = tempfile.mkdtemp()
    # simulation
    elif int(env_code) == 2:
        BASE_DIR = os.getcwd()
        # simulated db
        db = make_broker()
    else:
        # beamline
        BASE_DIR = os.path.expanduser('~/')

    # top directories
    HOME_DIR = os.path.join(BASE_DIR, HOME_DIR_NAME)
    BLCONFIG_DIR = os.path.join(BASE_DIR, BLCONFIG_DIR_NAME)
    ARCHIVE_BASE_DIR = os.path.join(BASE_DIR, ARCHIVE_BASE_DIR_NAME)

    # aquire object directories
    CONFIG_BASE = os.path.join(HOME_DIR, 'config_base')
    # copying pyFAI calib dict yml for test
    if int(env_code) == 1:
        a = os.path.dirname(os.path.abspath(__file__))
        b = a.split('glbl.py')[0]
        os.makedirs(CONFIG_BASE, exist_ok=True)
        shutil.copyfile(os.path.join(b, 'tests/pyFAI_calib.yml'),
                        os.path.join(CONFIG_BASE, 'pyFAI_calib.yml'))
    YAML_DIR = os.path.join(HOME_DIR, 'config_base', 'yml')
    BT_DIR = YAML_DIR
    SAMPLE_DIR = os.path.join(YAML_DIR, 'samples')
    EXPERIMENT_DIR = os.path.join(YAML_DIR, 'experiments')
    SCANPLAN_DIR = os.path.join(YAML_DIR, 'scanplans')
    # other dirs
    IMPORT_DIR = os.path.join(HOME_DIR, 'Import')
    ANALYSIS_DIR = os.path.join(HOME_DIR, 'userAnalysis')
    USERSCRIPT_DIR = os.path.join(HOME_DIR, 'userScripts')
    TIFF_BASE = os.path.join(HOME_DIR, 'tiff_base')
    USER_BACKUP_DIR = os.path.join(ARCHIVE_BASE_DIR, USER_BACKUP_DIR_NAME)

    ALL_FOLDERS = [
        HOME_DIR,
        BLCONFIG_DIR,
        YAML_DIR,
        CONFIG_BASE,
        SAMPLE_DIR,
        EXPERIMENT_DIR,
        SCANPLAN_DIR,
        TIFF_BASE,
        USERSCRIPT_DIR,
        IMPORT_DIR,
        ANALYSIS_DIR
    ]

    for folder in ALL_FOLDERS:
        os.makedirs(folder, exist_ok=True)

    # directories that won't be tar in the end of beamtime
    _EXCLUDE_DIR = [HOME_DIR, BLCONFIG_DIR, YAML_DIR]
    _EXPORT_TAR_DIR = [CONFIG_BASE, USERSCRIPT_DIR]

    class Glbl:
        beamline_host_name = BEAMLINE_HOST_NAME
        base = BASE_DIR
        home = HOME_DIR
        _export_tar_dir = _EXPORT_TAR_DIR
        xpdconfig = BLCONFIG_DIR
        import_dir = IMPORT_DIR
        config_base = CONFIG_BASE
        tiff_base = TIFF_BASE
        usrScript_dir = USERSCRIPT_DIR
        usrAnalysis_dir = ANALYSIS_DIR
        yaml_dir = YAML_DIR
        bt_dir = BT_DIR
        sample_dir = SAMPLE_DIR
        experiment_dir = EXPERIMENT_DIR
        scanplan_dir = SCANPLAN_DIR
        allfolders = ALL_FOLDERS
        archive_dir = USER_BACKUP_DIR
        owner = OWNER
        beamline_id = BEAMLINE_ID
        group = GROUP
        det_image_field = DET_IMAGE_FIELD
        dark_field_key = DARK_FIELD_KEY
        calib_config_name = CALIB_CONFIG_NAME
        exp_db = db
        # default masking dict
        mask_dict = {'edge': 30, 'lower_thresh': 0.0,
                     'upper_thresh': None, 'bs_width': 13,
                     'tri_offset': 13, 'v_asym': 0,
                     'alpha': 2.5, 'tmsk': None}
    return Glbl

env_code = os.environ['XPDAN_SETUP']
print('ENV_CODE = {}'.format(env_code))
an_glbl = make_glbl(env_code)
