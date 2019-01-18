import time

import pytest
from shed.simple import SimpleToEventStream as ToEventStream

from rapidz import Stream, move_to_first, destroy_pipeline
from rapidz.link import link
from xpdan.pipelines.main import pipeline_order

@pytest.mark.parametrize("exception", [True, False])
@pytest.mark.parametrize("background", [True, False])
@pytest.mark.parametrize("pe2", [True, False])
def test_main_pipeline(
    exp_db,
    fast_tmp_dir,
    start_uid3,
    start_uid1,
    start_uid2,
    background,
    exception,
    pe2
):
    namespace = link(
        *pipeline_order, raw_source=Stream(stream_name="raw source"),
        db=exp_db,
    )
    iq_em = ToEventStream(
        namespace["mean"].combine_latest(namespace["q"], emit_on=0),
        ("iq", "q"))
    iq_em.sink(print)

    limg = []
    move_to_first(namespace["bg_corrected_img"].sink(lambda x: limg.append(x)))
    lbgc = namespace["mean"].sink_to_list()
    lpdf = namespace["iq_comp"].sink_to_list()
    t0 = time.time()
    if background:
        uid = start_uid1
    elif pe2:
        uid = start_uid2
    else:
        uid = -1
    for nd in exp_db[uid].documents(fill=True):
        name, doc = nd
        if name == "start":
            if exception:
                doc["bt_wavelength"] = "bla"
            nd = (name, doc)
        try:
            namespace["raw_source"].emit(nd)
        except ValueError:
            pass
    t1 = time.time()
    print(t1 - t0)
    n_events = len(list(exp_db[-1].events()))
    assert len(limg) == n_events
    if exception:
        assert_lbgc = 0
    else:
        assert_lbgc = n_events
    assert len(lbgc) == assert_lbgc
    assert len(lpdf) == assert_lbgc
    assert iq_em.state == "stopped"
    destroy_pipeline(namespace["raw_source"])
    del namespace
    limg.clear()
    lbgc.clear()
    lpdf.clear()
