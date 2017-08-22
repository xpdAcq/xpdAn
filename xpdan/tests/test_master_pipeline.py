from xpdan.pipelines.master import conf_master_pipeline
import matplotlib.pyplot as plt
import os
from pprint import pprint


def test_master_pipeline(exp_db, fast_tmp_dir, start_uid3):
    """Decider between pipelines"""

    source = conf_master_pipeline(exp_db, fast_tmp_dir, vis=False,
                                  write_to_disk=True)
    for nd in exp_db[-1].documents(fill=True):
        source.emit(nd)
    assert 'Au' in os.listdir(fast_tmp_dir)
    assert 'Au_{}_md.yml'.format(start_uid3) in os.listdir(
        os.path.join(fast_tmp_dir, 'Au'))
    for f in ['dark_sub', 'mask', 'iq', 'pdf']:
        assert f in os.listdir(
            os.path.join(fast_tmp_dir, 'Au'))
