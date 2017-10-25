import os
import time

from xpdan.pipelines.callback import MainCallback


def test_main_pipeline(exp_db, fast_tmp_dir, start_uid3):
    """Decider between pipelines"""

    source = MainCallback(exp_db, fast_tmp_dir,
                          vis=True,
                          write_to_disk=True,
                          mask_setting=None,
                          )
    # source.visualize('/home/christopher/dev/xpdAn/examples/mystream.png')
    t0 = time.time()
    for nd in exp_db[-1].documents(fill=True):
        source(*nd)
    t1 = time.time()
    print(t1 - t0)
    for root, dirs, files in os.walk(fast_tmp_dir):
        level = root.replace(fast_tmp_dir, '').count(os.sep)
        indent = ' ' * 4 * level
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f))
    assert 'Au' in os.listdir(fast_tmp_dir)
    assert 'Au_{:.6}.yml'.format(start_uid3) in os.listdir(
        os.path.join(fast_tmp_dir, 'Au'))
    for f in ['dark_sub', 'mask', 'iq_q', 'iq_tth', 'pdf']:
        assert f in os.listdir(
            os.path.join(fast_tmp_dir, 'Au'))


def test_main_callback(exp_db, fast_tmp_dir, start_uid3):
    """Decider between pipelines"""

    source = MainCallback(exp_db, fast_tmp_dir,
                          mask_setting=None)
    # source.visualize('/home/christopher/dev/xpdAn/examples/mystream.png')
    t0 = time.time()
    for nd in exp_db[-1].documents(fill=True):
        print(nd)
        source(*nd)
    t1 = time.time()
    print(t1 - t0)
    for root, dirs, files in os.walk(fast_tmp_dir):
        level = root.replace(fast_tmp_dir, '').count(os.sep)
        indent = ' ' * 4 * level
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f))
    assert 'Au' in os.listdir(fast_tmp_dir)
    assert 'Au_{:.6}.yml'.format(start_uid3) in os.listdir(
        os.path.join(fast_tmp_dir, 'Au'))
    for f in ['dark_sub', 'mask', 'iq_q', 'iq_tth', 'pdf']:
        assert f in os.listdir(
            os.path.join(fast_tmp_dir, 'Au'))
