from xpdan.pipelines.dark_subtraction_logic import (configure_dark_sub)
from numpy.testing import assert_allclose
import numpy as np


def test_dark_subtraction_logic(exp_db):
    (fg_dark_stream_source, dark_query,
     dark_query_results,
     dark_sub_fg
     ) = configure_dark_sub(exp_db)

    L = dark_sub_fg.sink_to_list()

    for nd in exp_db[-1].documents(fill=True):
        fg_dark_stream_source.emit(nd)
    for n, d in L:
        if n == 'event':
            assert_allclose(d['data']['img'], np.zeros(d['data']['img'].shape))
