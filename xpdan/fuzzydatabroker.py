"""Create FuzzyDataBroker instance from configuration files"""
##############################################################################
#
# xpdan            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
from .fuzzybroker import FuzzyBroker
import warnings
try:
    import metadatastore.conf

    mds_config = metadatastore.conf.load_configuration(
        'metadatastore', 'MDS', ['host', 'database', 'port', 'timezone'])

    import filestore.conf

    fs_config = filestore.conf.load_configuration('filestore', 'FS',
                                                  ['host', 'database', 'port'])

    from filestore.fs import FileStoreRO
    from metadatastore.mds import MDSRO
except (KeyError, ImportError) as exc:
    warnings.warn("No default DataBroker object will be created because "
                  "the necessary configuration was not found: %s" % exc)

else:
    DataBroker = FuzzyBroker(MDSRO(mds_config), FileStoreRO(fs_config))
