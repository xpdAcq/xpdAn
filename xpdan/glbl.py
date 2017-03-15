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
import tempfile
from time import strftime

import matplotlib
from xpdan.simulation import build_pymongo_backed_broker

matplotlib.use('qt4agg')


def make_glbl(env_code=0, db=None):
    """ make a instance of Glbl class

    Glbl class is used to handle attributes and directories
     depends on environment variable

    Parameters
    ----------
    env_code : int
        environment variable to specify current situation

    Note
    ----
    by default: env_var 0 means beamline, 1 means test, 2 means simulation
    """

    home_dir_name = 'xpdUser'
    blconfig_dir_name = 'xpdConfig'
    beamline_host_name = 'xf28id1-ws2'
    archive_base_dir_name = '/direct/XF28ID1/pe1_data/.userBeamtimeArchive'
    user_backup_dir_name = strftime('%Y')
    owner = 'xf28id1'
    beamline_id = 'xpd'
    group = 'XPD'
    det_image_field = 'pe1_image'
    dark_field_key = 'sc_dk_field_uid'
    calib_config_name = 'pyFAI_calib.yml'

    # change this to be handled by an environment variable later
    if int(env_code) == 1:
        # test
        base_dir = tempfile.mkdtemp()
        print('creating {}'.format(base_dir))
    elif int(env_code) == 2:
        # simulation
        base_dir = os.getcwd()
        db = build_pymongo_backed_broker()
    else:
        # beamline
        base_dir = os.path.abspath('/direct/XF28ID1/pe2_data')
        from databroker.databroker import DataBroker as db

    # top directories
    home_dir = os.path.join(base_dir, home_dir_name)
    blconfig_dir = os.path.join(base_dir, blconfig_dir_name)
    archive_base_dir = os.path.abspath(archive_base_dir_name)

    # aquire object directories
    config_base = os.path.join(home_dir, 'config_base')
    # copying pyFAI calib dict yml for test
    if int(env_code) == 1:
        a = os.path.dirname(os.path.abspath(__file__))
        b = a.split('glbl.py')[0]
        os.makedirs(config_base, exist_ok=True)
        shutil.copyfile(os.path.join(b, 'tests/pyFAI_calib.yml'),
                        os.path.join(config_base, 'pyFAI_calib.yml'))
    yaml_dir = os.path.join(home_dir, 'config_base', 'yml')
    bt_dir = yaml_dir
    sample_dir = os.path.join(yaml_dir, 'samples')
    experiment_dir = os.path.join(yaml_dir, 'experiments')
    scanplan_dir = os.path.join(yaml_dir, 'scanplans')
    # other dirs
    import_dir = os.path.join(home_dir, 'Import')
    analysis_dir = os.path.join(home_dir, 'userAnalysis')
    userscript_dir = os.path.join(home_dir, 'userScripts')
    tiff_base = os.path.join(home_dir, 'tiff_base')
    user_backup_dir = os.path.join(archive_base_dir, user_backup_dir_name)

    all_folders = [
        home_dir,
        blconfig_dir,
        yaml_dir,
        config_base,
        sample_dir,
        experiment_dir,
        scanplan_dir,
        tiff_base,
        userscript_dir,
        import_dir,
        analysis_dir
    ]

    # only create dirs if running test
    if int(env_code) ==1:
        for folder in all_folders:
            os.makedirs(folder, exist_ok=True)

    # directories that won't be tar in the end of beamtime
    _exclude_dir = [home_dir, blconfig_dir, yaml_dir]
    _export_tar_dir = [config_base, userscript_dir]

    class Glbl:
        beamline_host_name = beamline_host_name
        base = base_dir
        home = home_dir
        _export_tar_dir = _export_tar_dir
        xpdconfig = blconfig_dir
        import_dir = import_dir
        config_base = config_base
        tiff_base = tiff_base
        usrScript_dir = userscript_dir
        usrAnalysis_dir = analysis_dir
        yaml_dir = yaml_dir
        bt_dir = bt_dir
        sample_dir = sample_dir
        experiment_dir = experiment_dir
        scanplan_dir = scanplan_dir
        allfolders = all_folders
        archive_dir = user_backup_dir
        owner = owner
        beamline_id = beamline_id
        group = group
        det_image_field = det_image_field
        dark_field_key = dark_field_key
        calib_config_name = calib_config_name
        exp_db = db
        # default masking dict
        mask_dict = {'edge': 30, 'lower_thresh': 0.0,
                     'upper_thresh': None, 'bs_width': 13,
                     'tri_offset': 13, 'v_asym': 0,
                     'alpha': 2.5, 'tmsk': None}

    return Glbl


try:
    env_code = os.environ['XPDAN_SETUP']
except KeyError:
    env_code = 1
print('ENV_CODE = {}'.format(env_code))
an_glbl = make_glbl(env_code)
