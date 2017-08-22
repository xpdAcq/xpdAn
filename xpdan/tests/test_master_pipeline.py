import os

from xpdan.pipelines.master import conf_master_pipeline


def test_master_pipeline(exp_db, fast_tmp_dir, start_uid3):
    """Decider between pipelines"""

    source = conf_master_pipeline(exp_db, fast_tmp_dir,
                                  # vis=False,
                                  write_to_disk=True)
    source.visualize('/home/christopher/dev/xpdAn/examples/mystream.png')
    for nd in exp_db[-1].documents(fill=True):
        source.emit(nd)
    for root, dirs, files in os.walk(fast_tmp_dir):
        level = root.replace(fast_tmp_dir, '').count(os.sep)
        indent = ' ' * 4 * level
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f))
    assert 'Au' in os.listdir(fast_tmp_dir)
    assert 'Au_{:.6}_md.yml'.format(start_uid3) in os.listdir(
        os.path.join(fast_tmp_dir, 'Au'))
    for f in ['dark_sub', 'mask', 'iq', 'pdf']:
        assert f in os.listdir(
            os.path.join(fast_tmp_dir, 'Au'))