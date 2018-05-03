import os
import time

from xpdan.pipelines.extra import z_score_plot
from xpdan.pipelines.main import (raw_source, filler, bg_query, bg_dark_query,
                                  fg_dark_query, mean, iq_comp)


def test_main_pipeline(exp_db, fast_tmp_dir, start_uid3):
    # reset the DBs so we can use the actual db
    filler.db = exp_db
    for a in [bg_query, bg_dark_query, fg_dark_query]:
        a.kwargs['db'] = exp_db

    lbgc = mean.sink_to_list()
    lpdf = iq_comp.sink_to_list()
    t0 = time.time()
    for nd in exp_db[-1].documents(fill=True):
        # Hack to change the output dir to the fast_tmp_dir
        name, doc = nd
        if name == 'start':
            doc.update(save_dir=fast_tmp_dir,
                       folder_tag_list=['save_dir'] + doc['folder_tag_list'])
            nd = (name, doc)
        raw_source.emit(nd)
    assert z_score_plot.upstreams[0].start_uid is None
    assert z_score_plot.upstreams[0].stopped
    t1 = time.time()
    print(t1 - t0)
    n_events = len(list(exp_db[-1].events()))
    assert len(lbgc) == n_events
    assert len(lpdf) == n_events
    for root, dirs, files in os.walk(fast_tmp_dir):
        level = root.replace(fast_tmp_dir, '').count(os.sep)
        indent = ' ' * 4 * level
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f))
    print(os.listdir(fast_tmp_dir))
    print(os.listdir(os.path.join(fast_tmp_dir, 'Au')))
    assert 'Au' in os.listdir(fast_tmp_dir)
    for f in ['dark_sub', 'mask', 'iq', 'itth', 'pdf']:
        assert f in os.listdir(
            os.path.join(fast_tmp_dir, 'Au'))
        assert len(os.listdir(os.path.join(fast_tmp_dir, 'Au', f))) == n_events
    assert 'Au_{:.6}.yaml'.format(start_uid3) in os.listdir(
        os.path.join(fast_tmp_dir, 'Au'))
