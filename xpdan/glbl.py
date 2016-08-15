import os
import socket
import yaml
import time
from time import strftime

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

# change this to be handled by an environment variable later
hostname = socket.gethostname()
if hostname == BEAMLINE_HOST_NAME:
    simulation = False
else:
    simulation = True

if simulation:
    BASE_DIR = os.getcwd()
else:
    BASE_DIR = os.path.expanduser('~/')

# top directories
HOME_DIR = os.path.join(BASE_DIR, HOME_DIR_NAME)
BLCONFIG_DIR = os.path.join(BASE_DIR, BLCONFIG_DIR_NAME)
ARCHIVE_BASE_DIR = os.path.join(BASE_DIR,ARCHIVE_BASE_DIR_NAME)

# aquire object directories
CONFIG_BASE = os.path.join(HOME_DIR, 'config_base')
YAML_DIR = os.path.join(HOME_DIR, 'config_base', 'yml')
BT_DIR = YAML_DIR
SAMPLE_DIR  = os.path.join(YAML_DIR, 'samples')
EXPERIMENT_DIR  = os.path.join(YAML_DIR, 'experiments')
SCANPLAN_DIR  = os.path.join(YAML_DIR, 'scanplans')
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

# directories that won't be tar in the end of beamtime
_EXCLUDE_DIR = [HOME_DIR, BLCONFIG_DIR, YAML_DIR]
_EXPORT_TAR_DIR = [CONFIG_BASE, USERSCRIPT_DIR]

class Glbl:
    _is_simulation = simulation
    beamline_host_name = BEAMLINE_HOST_NAME
    base = BASE_DIR
    home = HOME_DIR
    _export_tar_dir = _EXPORT_TAR_DIR
    xpdconfig = BLCONFIG_DIR
    import_dir = IMPORT_DIR
    config_base = CONFIG_BASE
    tiff_base =TIFF_BASE
    usrScript_dir = USERSCRIPT_DIR
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

glbl = Glbl()
