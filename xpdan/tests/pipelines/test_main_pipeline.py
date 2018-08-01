import os
import time

from xpdan.pipelines.main import (raw_source, filler, bg_query, bg_dark_query,
                                  fg_dark_query, mean, iq_comp)
from xpdan.pipelines.save import *

iq_em = ToEventStream(mean.combine_latest(q, emit_on=0), ('iq', 'q'))


def test_main_pipeline(exp_db, fast_tmp_dir, start_uid3):
    print('dir', fast_tmp_dir)
    save_kwargs.update({'base_folder': fast_tmp_dir})
    # reset the DBs so we can use the actual db
    filler.db = exp_db
    for a in [bg_query, bg_dark_query, fg_dark_query]:
        a.kwargs['db'] = exp_db

    lbgc = mean.sink_to_list()
    lpdf = iq_comp.sink_to_list()
    t0 = time.time()
    for nd in exp_db[-1].documents(fill=True):
        name, doc = nd
        if name == 'start':
            nd = (name, doc)
        raw_source.emit(nd)
    assert iq_em.stopped
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
        if f == 'mask':
            assert len(os.listdir(os.path.join(fast_tmp_dir, 'Au',
                                               f))) == n_events * 2
        else:
            assert len(os.listdir(os.path.join(fast_tmp_dir, 'Au',
                                               f))) == n_events
    assert 'Au_{:.6}.yaml'.format(start_uid3) in os.listdir(
        os.path.join(fast_tmp_dir, 'Au', 'meta'))


def test_main_pipeline_no_background(exp_db, fast_tmp_dir, start_uid1):
    save_kwargs.update({'base_folder': fast_tmp_dir})
    # reset the DBs so we can use the actual db
    filler.db = exp_db
    for a in [bg_query, bg_dark_query, fg_dark_query]:
        a.kwargs['db'] = exp_db

    lbgc = mean.sink_to_list()
    lpdf = iq_comp.sink_to_list()
    t0 = time.time()
    for nd in exp_db[start_uid1].documents(fill=True):
        # Hack to change the output dir to the fast_tmp_dir
        name, doc = nd
        if name == 'start':
            nd = (name, doc)
        raw_source.emit(nd)
    assert iq_em.stopped
    t1 = time.time()
    print(t1 - t0)
    n_events = len(list(exp_db[start_uid1].events()))
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
    print(os.listdir(os.path.join(fast_tmp_dir, 'kapton')))
    assert 'kapton' in os.listdir(fast_tmp_dir)
    for f in ['dark_sub', 'mask', 'iq', 'itth', 'pdf']:
        assert f in os.listdir(
            os.path.join(fast_tmp_dir, 'kapton'))
        if f == 'mask':
            assert len(os.listdir(os.path.join(fast_tmp_dir, 'kapton', f))
                       ) == n_events * 2
        else:
            assert len(os.listdir(os.path.join(fast_tmp_dir, 'kapton', f))
                       ) == n_events
    assert 'kapton_{:.6}.yaml'.format(start_uid1) in os.listdir(
        os.path.join(fast_tmp_dir, 'kapton', 'meta'))


def test_main_exception_pipeline(exp_db, fast_tmp_dir, start_uid3):
    print('dir', fast_tmp_dir)
    save_kwargs.update({'base_folder': fast_tmp_dir})
    # reset the DBs so we can use the actual db
    filler.db = exp_db
    for a in [bg_query, bg_dark_query, fg_dark_query]:
        a.kwargs['db'] = exp_db

    t0 = time.time()
    for nd in exp_db[-1].documents(fill=True):
        name, doc = nd
        if name == 'start':
            doc['bt_wavelength'] = 'bla'
        nd = (name, doc)
        try:
            raw_source.emit(nd)
        except ValueError:
            pass
    assert iq_em.stopped
    t1 = time.time()
    print(t1 - t0)
    n_events = len(list(exp_db[-1].events()))
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
    for f in ['dark_sub', 'mask']:
        assert f in os.listdir(os.path.join(fast_tmp_dir, 'Au'))
        if f == 'mask':
            assert len(os.listdir(os.path.join(fast_tmp_dir, 'Au', f))
                       ) == n_events * 2
        else:
            assert len(os.listdir(os.path.join(fast_tmp_dir, 'Au',
                                           f))) == n_events
    for f in ['itth', 'fq', 'iq', 'pdf']:
        assert f in os.listdir(os.path.join(fast_tmp_dir, 'Au'))
        assert len(os.listdir(os.path.join(fast_tmp_dir, 'Au',
                                           f))) == 0
    assert 'Au_{:.6}.yaml'.format(start_uid3) in os.listdir(
        os.path.join(fast_tmp_dir, 'Au', 'meta'))
