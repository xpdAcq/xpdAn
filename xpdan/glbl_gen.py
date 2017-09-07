import os
import shutil
import tempfile

import yaml
from databroker import Broker
try:
    db = Broker.named('xpd')
except (NameError, FileNotFoundError):
    from xpdsim import db
import logging
from pkg_resources import resource_filename as rs_fn

logger = logging.getLogger(__name__)
pytest_dir = rs_fn('xpdan', 'tests')


def load_configuration(name):
    """
    Load configuration data from a cascading series of locations.

    The precedence order is (highest priority last):

    1. The conda environment
       - CONDA_ENV/etc/{name}.yaml (if CONDA_ETC_ is defined for the env)
    2. The shipped version
    3. At the system level
       - /etc/{name}.yml
    4. In the user's home directory
       - ~/.config/{name}.yml

    where
        {name} is xpdan

    Parameters
    ----------
    name : str
        The expected base-name of the configuration files

    Returns
    ------
    conf : dict
        Dictionary keyed on ``fields`` with the values extracted
    """
    filenames = [
        os.path.join(rs_fn('xpdan', 'config/xpdan.yml')),
        os.path.join('/etc', name + '.yml'),
        os.path.join(os.path.expanduser('~'), '.config', name + '.yml'),
    ]

    if 'CONDA_ETC_' in os.environ:
        filenames.insert(0, os.path.join(
            os.environ['CONDA_ETC_'], name + '.yml'))

    config = {}
    for filename in filenames:
        if os.path.isfile(filename):
            with open(filename) as f:
                config.update(yaml.load(f))
            logger.debug("Using glbl specified in config file. \n%r",
                         config)
    return config


def make_glbl(config, env_code=0, db_xptal=None, db_an=None):
    """ make a glbl dict

    glbl dict is used to handle attributes and directories
     depends on environment variable

    Parameters
    ----------
    config: dict
        Configuration dictionary
    env_code: int, optional
        environment variable to specify current situation
    db_xptal: databroker.broker instance
        The experimental databroker
    db_an: databroker.broker instance
        The anaysis databroker

    Note
    ----
    by default: env_var 0 means beamline, 1 means test, 2 means simulation
    """

    # change this to be handled by an environment variable later
    if int(env_code) == 1:
        # test
        base_dir = tempfile.mkdtemp()
        print('creating {}'.format(base_dir))
    elif int(env_code) == 2:
        # simulation
        base_dir = os.getcwd()
        db_xptal = db
    else:
        # beamline
        base_dir = os.path.abspath('/direct/XF28ID1/pe2_data')
        from databroker.databroker import DataBroker as db_xptal

    config['base_dir'] = base_dir
    config['exp_db'] = db_xptal
    config['an_db'] = db_an
    # top directories
    config.update({k: os.path.join(config['base_dir'], config[z]) for k, z in
                   zip(['home_dir', 'blconfig_dir'],
                       ['home_dir_name', 'blconfig_dir_name'])})
    config['archive_base_dir'] = os.path.abspath(
        config['archive_base_dir_name'])

    # aquire object directories
    config.update(
        dict(config_base=os.path.join(config['home_dir'], 'config_base')))

    # copying xpdAcq_calib_info yml for test
    if int(env_code) == 1:
        os.makedirs(config['config_base'], exist_ok=True)
        shutil.copyfile(os.path.join(pytest_dir,
                                     'xpdAcq_calib_info.yml'),
                        os.path.join(config['config_base'],
                                     'xpdAcq_calib_info.yml'))

    config.update(
        dict(yaml_dir=os.path.join(config['home_dir'], 'config_base', 'yml')))
    config.update({k: os.path.join(config['home_dir'], z) for k, z in zip(
        ['import_dir', 'analysis_dir', 'userscript_dir',
         'tiff_base'],
        ['Import', 'userAnalysis', 'userScripts', 'tiff_base']
    )})

    config['bt_dir'] = config['yaml_dir']
    config.update({k: os.path.join(config['yaml_dir'], z) for k, z in zip(
        ['sample_dir', 'experiment_dir', 'scanplan_dir'],
        ['samples', 'experiments', 'scanplans']
    )})
    # other dirs
    config['user_backup_dir'] = os.path.join(config['archive_base_dir'],
                                             config['user_backup_dir_name'])

    config['all_folders'] = [config[k] for k in ['home_dir',
                                                 'blconfig_dir',
                                                 'yaml_dir',
                                                 'config_base',
                                                 'sample_dir',
                                                 'experiment_dir',
                                                 'scanplan_dir',
                                                 'tiff_base',
                                                 'userscript_dir',
                                                 'import_dir',
                                                 'analysis_dir']]

    # only create dirs if running test
    if int(env_code) == 1:
        print('making dirs')
        for folder in config['all_folders']:
            os.makedirs(folder, exist_ok=True)

    # directories that won't be tar in the end of beamtime
    config['_exclude_dir'] = [config[k] for k in
                              ['home_dir', 'blconfig_dir', 'yaml_dir']]
    config['_export_tar_dir'] = [config[k] for k in
                                 ['config_base', 'userscript_dir']]
    return config
