import time

import pytest
from shed.simple import SimpleToEventStream as ToEventStream
from rapidz import Stream, move_to_first, destroy_pipeline
from xpdan.pipelines.main import pipeline_order
from rapidz.link import link


@pytest.mark.parametrize("exception", [True, False])
@pytest.mark.parametrize("background", [True, False])
def test_main_pipeline(
    exp_db, fast_tmp_dir, start_uid3, start_uid1, background, exception
):
    namespace = link(
        *pipeline_order, raw_source=Stream(stream_name="raw source"),
        db=exp_db,
    )
    mean = namespace["mean"]
    iq_comp = namespace["iq_comp"]
    q = namespace["q"]
    raw_source = namespace["raw_source"]

    iq_em = ToEventStream(mean.combine_latest(q, emit_on=0), ("iq", "q"))
    iq_em.sink(print)

    limg = []
    move_to_first(namespace["bg_corrected_img"].sink(lambda x: limg.append(x)))
    lbgc = mean.sink_to_list()
    lpdf = iq_comp.sink_to_list()
    t0 = time.time()
    if background:
        uid = start_uid1
    else:
        uid = -1
    for nd in exp_db[uid].documents():
        name, doc = nd
        if name == "start":
            if exception:
                doc["bt_wavelength"] = "bla"
            nd = (name, doc)
        try:
            raw_source.emit(nd)
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
    destroy_pipeline(raw_source)
    limg.clear()
    lbgc.clear()
    lpdf.clear()
