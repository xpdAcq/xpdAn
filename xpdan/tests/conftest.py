import shutil
import sys
import uuid

import pytest
from databroker import Broker
from filestore.handlers import NpyHandler

from xpdan.tests.utils import insert_imgs
from xpdan.data_reduction import DataReduction
from xpdan.glbl import make_glbl

if sys.version_info >= (3, 0):
    pass


@pytest.fixture(params=[
    # 'sqlite',
    'mongo'], scope='function')
def db(request):
    param_map = {
        # 'sqlite': build_sqlite_backed_broker,
        'mongo': build_pymongo_backed_broker}

    return param_map[request.param](request)


@pytest.fixture(scope='function')
def handler(exp_db):
    handler = DataReduction(exp_db=exp_db)
    return handler


@pytest.fixture(scope='function')
def exp_db(db):
    glbl = make_glbl()
    db2 = next(db)
    mds = db2.mds
    fs = db2.fs
    insert_imgs(mds, fs, 5, (200, 200), glbl.base)
    yield db2
    print('removing {}'.format(glbl.base))
    shutil.rmtree(glbl.base)


def build_pymongo_backed_broker(request):
    """Provide a function level scoped MDS instance talking to
    temporary database on localhost:27017 with v1 schema.

    """
    from metadatastore.mds import MDS
    from filestore.utils import create_test_database
    from filestore.fs import FileStore

    db_name = "mds_testing_disposable_{}".format(str(uuid.uuid4()))
    mds_test_conf = dict(database=db_name, host='localhost',
                         port=27017, timezone='US/Eastern')
    try:
        mds = MDS(mds_test_conf, 1, auth=False)
    except:
        mds = MDS(mds_test_conf, 1)

    db_name = "fs_testing_base_disposable_{}".format(str(uuid.uuid4()))
    fs_test_conf = create_test_database(host='localhost',
                                        port=27017,
                                        version=1,
                                        db_template=db_name)
    fs = FileStore(fs_test_conf, version=1)
    fs.register_handler('npy', NpyHandler)

    yield Broker(mds, fs)

    print("DROPPING DB")
    mds._connection.drop_database(mds_test_conf['database'])
    print("DROPPING DB")
    fs._connection.drop_database(fs_test_conf['database'])
