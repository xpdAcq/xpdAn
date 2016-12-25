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
